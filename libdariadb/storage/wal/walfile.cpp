#ifdef MSVC
#define _CRT_SECURE_NO_WARNINGS // disable msvc /sdl warning on fopen call.
#endif
#include <libdariadb/flags.h>
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
  Private(const EngineEnvironment_ptr env) {
    _env = env;
    _settings = _env->getResourceObject<Settings>(
        EngineEnvironment::Resource::SETTINGS);
    _writed = 0;
    _is_readonly = false;
    auto rnd_fname = utils::fs::random_file_name(WAL_FILE_EXT);
    _filename = utils::fs::append_path(_settings->raw_path.value(), rnd_fname);
    _env->getResourceObject<Manifest>(EngineEnvironment::Resource::MANIFEST)
        ->wal_append(rnd_fname);
    _file = nullptr;
  }

  Private(const EngineEnvironment_ptr env, const std::string &fname,
          bool readonly) {
    _env = env;
    _settings = _env->getResourceObject<Settings>(
        EngineEnvironment::Resource::SETTINGS);
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
      return Status(0, 1);
    }
    open_to_append();
    std::fwrite(&value, sizeof(Meas), size_t(1), _file);
    std::fflush(_file);
    _writed++;
    return Status(1, 0);
  }

  Status append(const MeasArray::const_iterator &begin,
                const MeasArray::const_iterator &end) {
    ENSURE(!_is_readonly);

    auto sz = std::distance(begin, end);
    open_to_append();
    auto max_size = _settings->wal_file_size.value();
    auto write_size = (sz + _writed) > max_size ? (max_size - _writed) : sz;
    std::fwrite(&(*begin), sizeof(Meas), write_size, _file);
    std::fflush(_file);
    _writed += write_size;
    return Status(write_size, 0);
  }

  Status append(const MeasList::const_iterator &begin,
                const MeasList::const_iterator &end) {
    ENSURE(!_is_readonly);

    auto list_size = std::distance(begin, end);
    open_to_append();

    auto max_size = _settings->wal_file_size.value();

    auto write_size =
        (list_size + _writed) > max_size ? (max_size - _writed) : list_size;
    MeasArray ma{begin, end};
    std::fwrite(ma.data(), sizeof(Meas), write_size, _file);
    std::fflush(_file);
    _writed += write_size;
    return Status(write_size, 0);
  }

  Statistic stat(const Id id, Time from, Time to) {
    Statistic result;
    open_to_read();

    IdArray ids{id};
    ENSURE(ids[0] == id);
    while (1) {
      Meas val = Meas();
      if (fread(&val, sizeof(Meas), size_t(1), _file) == 0) {
        break;
      }
      if (val.inQuery(ids, Flag(0), from, to)) {
        result.update(val);
      }
    }
    std::fclose(_file);
    _file = nullptr;

    return result;
  }

  Id2Cursor intervalReader(const QueryInterval &q) {
    open_to_read();

    Id2MSet subresult;

    while (1) {
      Meas val = Meas();
      if (fread(&val, sizeof(Meas), size_t(1), _file) == 0) {
        break;
      }
      if (val.inQuery(q.ids, q.flag, q.from, q.to)) {
        subresult[val.id].insert(val);
      }
    }
    std::fclose(_file);
    _file = nullptr;

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

    open_to_read();

    while (1) {
      Meas val = Meas();
      if (fread(&val, sizeof(Meas), size_t(1), _file) == 0) {
        break;
      }
      if (val.inQuery(q.ids, q.flag) && (val.time <= q.time_point)) {
        replace_if_older(sub_res, val);
        readed_ids.insert(val.id);
      }
    }
    std::fclose(_file);
    _file = nullptr;

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

    open_to_read();
    while (1) {
      Meas val = Meas();
      if (fread(&val, sizeof(Meas), size_t(1), _file) == 0) {
        break;
      }
      if (val.inFlag(flag) && val.inIds(ids)) {
        replace_if_older(sub_res, val);
        readed_ids.emplace(val.id);
      }
    }
    std::fclose(_file);
    _file = nullptr;

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

  dariadb::Time minTime() {
    open_to_read();

    dariadb::Time result = dariadb::MAX_TIME;

    while (1) {
      Meas val = Meas();
      if (fread(&val, sizeof(Meas), size_t(1), _file) == 0) {
        break;
      }
      result = std::min(val.time, result);
    }
    std::fclose(_file);
    _file = nullptr;
    return result;
  }

  dariadb::Time maxTime() {
    open_to_read();

    dariadb::Time result = dariadb::MIN_TIME;

    while (1) {
      Meas val = Meas();
      if (fread(&val, sizeof(Meas), size_t(1), _file) == 0) {
        break;
      }
      result = std::max(val.time, result);
    }
    std::fclose(_file);
    _file = nullptr;
    return result;
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) {
    open_to_read();

    *minResult = dariadb::MAX_TIME;
    *maxResult = dariadb::MIN_TIME;
    bool result = false;
    while (1) {
      Meas val = Meas();
      if (fread(&val, sizeof(Meas), size_t(1), _file) == 0) {
        break;
      }
      if (val.id == id) {
        result = true;
        *minResult = std::min(*minResult, val.time);
        *maxResult = std::max(*maxResult, val.time);
      }
    }
    std::fclose(_file);
    _file = nullptr;
    return result;
  }

  void flush() {}

  std::string filename() const { return _filename; }

  std::shared_ptr<MeasArray> readAll() {
    open_to_read();

    auto ma = std::make_shared<MeasArray>(_settings->wal_file_size.value());
    auto raw = ma.get();
    size_t pos = 0;
    while (1) {
      Meas val = Meas();
      if (fread(&val, sizeof(Meas), size_t(1), _file) == 0) {
        break;
      }
      (*raw)[pos] = val;
      pos++;
    }
    ma->resize(pos);
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
      ss << f << std::endl;
    }
    auto wals_exists = utils::fs::ls(_settings->raw_path.value(), WAL_FILE_EXT);
    for (auto f : wals_exists) {
      ss << f << std::endl;
    }
    throw MAKE_EXCEPTION(ss.str());
  }

  Id2MinMax loadMinMax() {
    open_to_read();
    Id2MinMax result;
    while (1) {
      Meas val = Meas();
      if (fread(&val, sizeof(Meas), size_t(1), _file) == 0) {
        break;
      }

      auto fres = result.find(val.id);
      if (fres == result.end()) {
        result[val.id].min = val;
        result[val.id].max = val;
      } else {
        fres->second.updateMax(val);
        fres->second.updateMin(val);
      }
    }
    std::fclose(_file);
    _file = nullptr;
    return result;
  }

protected:
  std::string _filename;
  bool _is_readonly;
  size_t _writed;
  EngineEnvironment_ptr _env;
  Settings *_settings;
  FILE *_file;
};

WALFile_Ptr WALFile::create(const EngineEnvironment_ptr env) {
  return WALFile_Ptr{new WALFile(env)};
}

WALFile_Ptr WALFile::open(const EngineEnvironment_ptr env,
                          const std::string &fname, bool readonly) {
  return WALFile_Ptr{new WALFile(env, fname, readonly)};
}

WALFile::~WALFile() {}

WALFile::WALFile(const EngineEnvironment_ptr env)
    : _Impl(new WALFile::Private(env)) {}

WALFile::WALFile(const EngineEnvironment_ptr env, const std::string &fname,
                 bool readonly)
    : _Impl(new WALFile::Private(env, fname, readonly)) {}

dariadb::Time WALFile::minTime() { return _Impl->minTime(); }

dariadb::Time WALFile::maxTime() { return _Impl->maxTime(); }

bool WALFile::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                         dariadb::Time *maxResult) {
  return _Impl->minMaxTime(id, minResult, maxResult);
}
void WALFile::flush() { // write all to storage;
  _Impl->flush();
}

Status WALFile::append(const Meas &value) { return _Impl->append(value); }
Status WALFile::append(const MeasArray::const_iterator &begin,
                       const MeasArray::const_iterator &end) {
  return _Impl->append(begin, end);
}
Status WALFile::append(const MeasList::const_iterator &begin,
                       const MeasList::const_iterator &end) {
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

std::string WALFile::filename() const { return _Impl->filename(); }

std::shared_ptr<MeasArray> WALFile::readAll() { return _Impl->readAll(); }

size_t WALFile::writed(std::string fname) {
  std::ifstream in(fname, std::ifstream::ate | std::ifstream::binary);
  return in.tellg() / sizeof(Meas);
}

Id2MinMax WALFile::loadMinMax() { return _Impl->loadMinMax(); }
