#ifdef MSVC
#define _CRT_SECURE_NO_WARNINGS // disable msvc /sdl warning on fopen call.
#endif
#include <libdariadb/flags.h>
#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/callbacks.h>
#include <libdariadb/storage/cursors.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/storage/wal/walfile.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/utils.h>

#include <algorithm>

#include <cstdio>
#include <cstring>
#include <fstream>
#include <mutex>
#include <sstream>

using namespace dariadb;
using namespace dariadb::storage;

class WALFile::Private {
public:
  Private(const EngineEnvironment_ptr env, dariadb::Id id) {
    _env = env;
    _settings = _env->getResourceObject<Settings>(EngineEnvironment::Resource::SETTINGS);
    _writed = 0;
    _is_readonly = false;
    auto rnd_fname = utils::fs::random_file_name(WAL_FILE_EXT);
    _filename = utils::fs::append_path(_settings->raw_path.value(), rnd_fname);
    _env->getResourceObject<Manifest>(EngineEnvironment::Resource::MANIFEST)
        ->wal_append(rnd_fname, id);
    _file = nullptr;
    _idBloom = bloom_empty<Id>();
  }

  Private(const EngineEnvironment_ptr env, const std::string &fname, bool readonly) {
    _env = env;
    _settings = _env->getResourceObject<Settings>(EngineEnvironment::Resource::SETTINGS);
    _writed = WALFile::writed(fname);
    _is_readonly = readonly;
    _filename = fname;
    _file = nullptr;
  }

  ~Private() {
    this->flush();
    if (_file != nullptr) {
      std::fclose(_file);
      _file = nullptr;
    }
  }

  void open_to_append() {
    if (_file != nullptr) {
      return;
    }

    _file = std::fopen(_filename.c_str(), "ab");
    if (_file == nullptr) {
      throw MAKE_EXCEPTION("WALFile: open_to_append error.");
    }
  }

  void open_to_read() {
    if (_file != nullptr) {
      std::fclose(_file);
      _file = nullptr;
    }
    _file = std::fopen(_filename.c_str(), "rb");
    if (_file == nullptr) {
      throw_open_error_exception();
    }
  }

  Status append(const Meas &value) {
    ENSURE(!_is_readonly);

    if (_writed > _settings->wal_file_size.value()) {
      return Status(1, APPEND_ERROR::wal_file_limit);
    }
    open_to_append();
    std::fwrite(&value, sizeof(Meas), size_t(1), _file);
    std::fflush(_file);
    _minTime = std::min(_minTime, value.time);
    _maxTime = std::max(_maxTime, value.time);
    _writed++;
    _idBloom = bloom_add<Id>(_idBloom, value.id);
    return Status(1);
  }

  Status append(const MeasArray::const_iterator &begin,
                const MeasArray::const_iterator &end) {
    ENSURE(!_is_readonly);
	
    auto sz = std::distance(begin, end);
    open_to_append();
    auto max_size = _settings->wal_file_size.value();
    auto write_size = (sz + _writed) > max_size ? (max_size - _writed) : sz;
	if (write_size == size_t()) {
		Status result;
		result.ignored = sz;
		result.error = APPEND_ERROR::wal_file_limit;
		return result;
	}
    std::fwrite(&(*begin), sizeof(Meas), write_size, _file);
    std::fflush(_file);
    for (auto it = begin; it != begin + write_size; ++it) {
      auto value = *it;
      _minTime = std::min(_minTime, value.time);
      _maxTime = std::max(_maxTime, value.time);
      _idBloom = bloom_add<Id>(_idBloom, value.id);
    }
    _writed += write_size;
    return Status(write_size);
  }

  Statistic stat(const Id id, Time from, Time to) {
    Statistic result;

    IdArray ids{id};
    ENSURE(ids[0] == id);
    auto all = readAll();
    for (size_t i = 0; i < all->size(); ++i) {
      auto val = all->at(i);
      if (val.inQuery(ids, Flag(0), from, to)) {
        result.update(val);
      }
    }

    return result;
  }

  Id2Cursor intervalReader(const QueryInterval &q) {
    Id2MSet subresult;
    auto all = readAll();
    for (size_t i = 0; i < all->size(); ++i) {
      auto val = all->at(i);
      if (val.inQuery(q.ids, q.flag, q.from, q.to)) {
        subresult[val.id].insert(val);
      }
    }

    if (subresult.empty()) {
      return Id2Cursor();
    }

    Id2Cursor result;
    for (auto kv : subresult) {
      MeasArray ma(kv.second.begin(), kv.second.end());
      std::sort(ma.begin(), ma.end(), meas_time_compare_less());
      ENSURE(ma.front().time <= ma.back().time);
      FullCursor *fr = new FullCursor(ma);
      Cursor_Ptr reader{fr};
      result[kv.first] = reader;
    }
    return result;
  }

  void foreach (const QueryInterval &q, IReadCallback * clbk) {
    auto readers = intervalReader(q);

    for (auto kv : readers) {
      kv.second->apply(clbk);
    }
  }

  Id2Meas readTimePoint(const QueryTimePoint &q) {
    dariadb::IdSet readed_ids;
    dariadb::Id2Meas sub_res;

    auto all = readAll();
    for (size_t i = 0; i < all->size(); ++i) {
      auto val = all->at(i);
      if (val.inQuery(q.ids, q.flag) && (val.time <= q.time_point)) {
        replace_if_older(sub_res, val);
        readed_ids.insert(val.id);
      }
    }

    if (!q.ids.empty() && readed_ids.size() != q.ids.size()) {
      for (auto id : q.ids) {
        if (readed_ids.find(id) == readed_ids.end()) {
          auto e = Meas(id);
          e.flag = FLAGS::_NO_DATA;
          e.time = q.time_point;
          sub_res[id] = e;
        }
      }
    }

    return sub_res;
  }

  void replace_if_older(dariadb::Id2Meas &s, const dariadb::Meas &m) const {
    auto fres = s.find(m.id);
    if (fres == s.end()) {
      s.emplace(std::make_pair(m.id, m));
    } else {
      if (fres->second.time < m.time) {
        s.emplace(std::make_pair(m.id, m));
      }
    }
  }

  Id2Meas currentValue(const IdArray &ids, const Flag &flag) {
    dariadb::Id2Meas sub_res;
    dariadb::IdSet readed_ids;

    auto all = readAll();
    for (size_t i = 0; i < all->size(); ++i) {
      auto val = all->at(i);
      if (val.inFlag(flag) && val.inIds(ids)) {
        replace_if_older(sub_res, val);
        readed_ids.emplace(val.id);
      }
    }

    if (!ids.empty() && readed_ids.size() != ids.size()) {
      for (auto id : ids) {
        if (readed_ids.find(id) == readed_ids.end()) {
          auto e = Meas(id);
          e.flag = FLAGS::_NO_DATA;
          e.time = dariadb::Time(0);
          sub_res[id] = e;
        }
      }
    }

    return sub_res;
  }

  void updateBloom() {
    _idBloom = bloom_empty<Id>();
    auto all = this->readAll();
    for (auto v : *all) {
      _idBloom = bloom_add<Id>(_idBloom, v.id);
    }
  }

  dariadb::Id _idBloom = MIN_ID;
  Id id_bloom() {
    if (_idBloom == MIN_ID) {
      updateBloom();
    }
    return _idBloom;
  }

  dariadb::Time _minTime = MAX_TIME;
  dariadb::Time _maxTime = MIN_TIME;

  dariadb::Time minTime() {
    dariadb::Time result = dariadb::MAX_TIME;
    auto all = readAll();
    for (size_t i = 0; i < all->size(); ++i) {
      auto val = all->at(i);
      result = std::min(val.time, result);
    }
    return result;
  }

  dariadb::Time maxTime() {
    dariadb::Time result = dariadb::MIN_TIME;

    auto all = readAll();
    for (size_t i = 0; i < all->size(); ++i) {
      auto val = all->at(i);
      result = std::max(val.time, result);
    }
    return result;
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult, dariadb::Time *maxResult) {
    *minResult = dariadb::MAX_TIME;
    *maxResult = dariadb::MIN_TIME;

    bool result = false;
    auto all = readAll();
    for (size_t i = 0; i < all->size(); ++i) {
      auto val = all->at(i);
      if (val.id == id) {
        result = true;
        *minResult = std::min(*minResult, val.time);
        *maxResult = std::max(*maxResult, val.time);
      }
    }
    return result;
  }

  void flush() {}

  std::string filename() const { return _filename; }

  std::shared_ptr<MeasArray> readAll() {
    open_to_read();

    auto ma = std::make_shared<MeasArray>(_writed);
    auto raw = ma.get();
    auto result = fread(raw->data(), sizeof(Meas), _writed, _file);
    if (result < _writed) {
      THROW_EXCEPTION("result < _writed");
    }
    std::fclose(_file);
    _file = nullptr;
    return ma;
  }

  [[noreturn]] void throw_open_error_exception() const {
    std::stringstream ss;
    ss << "wal: file open error " << _filename;
    auto wals_manifest =
        _env->getResourceObject<Manifest>(EngineEnvironment::Resource::MANIFEST)
            ->wal_list();
    ss << "Manifest:";
    for (auto f : wals_manifest) {
      ss << f.fname << std::endl;
    }
    auto wals_exists = utils::fs::ls(_settings->raw_path.value(), WAL_FILE_EXT);
    for (auto f : wals_exists) {
      ss << f << std::endl;
    }
    throw MAKE_EXCEPTION(ss.str());
  }

  Id2MinMax_Ptr loadMinMax() {

    Id2MinMax_Ptr result = std::make_shared<Id2MinMax>();
    auto all = readAll();
    for (size_t i = 0; i < all->size(); ++i) {
      auto val = all->at(i);
      auto fres = result->find_bucket(val.id);

      fres.v->second.updateMax(val);
      fres.v->second.updateMin(val);
    }
    return result;
  }

  Id id_from_first() {
    auto all = readAll();
    if (all->empty()) {
      return MAX_ID;
    } else {
      return all->front().id;
    }
  }

  size_t writed()const {
	  return _writed;
  }
protected:
  std::string _filename;
  bool _is_readonly;
  size_t _writed;
  EngineEnvironment_ptr _env;
  Settings *_settings;
  FILE *_file;
};

WALFile_Ptr WALFile::create(const EngineEnvironment_ptr env, dariadb::Id id) {
  return WALFile_Ptr{new WALFile(env, id)};
}

WALFile_Ptr WALFile::open(const EngineEnvironment_ptr env, const std::string &fname,
                          bool readonly) {
  return WALFile_Ptr{new WALFile(env, fname, readonly)};
}

WALFile::~WALFile() {}

WALFile::WALFile(const EngineEnvironment_ptr env, dariadb::Id id) : _Impl(new WALFile::Private(env, id)) {}

WALFile::WALFile(const EngineEnvironment_ptr env, const std::string &fname, bool readonly)
    : _Impl(new WALFile::Private(env, fname, readonly)) {}

dariadb::Id WALFile::id_bloom() {
  return _Impl->id_bloom();
}

dariadb::Time WALFile::minTime() {
  return _Impl->minTime();
}

dariadb::Time WALFile::maxTime() {
  return _Impl->maxTime();
}

bool WALFile::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                         dariadb::Time *maxResult) {
  return _Impl->minMaxTime(id, minResult, maxResult);
}
void WALFile::flush() { // write all to storage;
  _Impl->flush();
}

Status WALFile::append(const Meas &value) {
  return _Impl->append(value);
}
Status WALFile::append(const MeasArray::const_iterator &begin,
                       const MeasArray::const_iterator &end) {
  return _Impl->append(begin, end);
}

Id2Cursor WALFile::intervalReader(const QueryInterval &q) {
  return _Impl->intervalReader(q);
}

Statistic WALFile::stat(const Id id, Time from, Time to) {
  return _Impl->stat(id, from, to);
}

void WALFile::foreach (const QueryInterval &q, IReadCallback * clbk) {
  return _Impl->foreach (q, clbk);
}

Id2Meas WALFile::readTimePoint(const QueryTimePoint &q) {
  return _Impl->readTimePoint(q);
}

Id2Meas WALFile::currentValue(const IdArray &ids, const Flag &flag) {
  return _Impl->currentValue(ids, flag);
}

std::string WALFile::filename() const {
  return _Impl->filename();
}

std::shared_ptr<MeasArray> WALFile::readAll() {
  return _Impl->readAll();
}

size_t WALFile::writed(std::string fname) {
  std::ifstream in(fname, std::ifstream::ate | std::ifstream::binary);
  return in.tellg() / sizeof(Meas);
}

Id2MinMax_Ptr WALFile::loadMinMax() {
  return _Impl->loadMinMax();
}

Id WALFile::id_from_first() {
  return _Impl->id_from_first();
}

size_t WALFile::writed()const {
	return _Impl->writed();
}