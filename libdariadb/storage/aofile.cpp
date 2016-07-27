#include "aofile.h"
#include "../flags.h"
#include "../utils/fs.h"
#include "../utils/metrics.h"
#include "callbacks.h"
#include "manifest.h"
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <mutex>

using namespace dariadb;
using namespace dariadb::storage;

class AOFile::Private {
public:
  Private(const AOFile::Params &params) : _params(params) {
    _writed = 0;
    _is_readonly = false;
    auto rnd_fname = utils::fs::random_file_name(AOF_FILE_EXT);
    _filename = utils::fs::append_path(_params.path, rnd_fname);
    Manifest::instance()->aof_append(rnd_fname);
    is_full = false;
  }

  Private(const AOFile::Params &params, const std::string &fname, bool readonly)
      : _params(params) {
    _writed = AOFile::writed(fname);
    _is_readonly = readonly;
    _filename = fname;
    is_full = false;
  }

  ~Private() { this->flush(); }

  append_result append(const Meas &value) {
    TIMECODE_METRICS(ctmd, "append", "AOFile::append");
    assert(!_is_readonly);
    std::lock_guard<std::mutex> lock(_mutex);
    if (_writed > _params.size) {
      return append_result(0, 1);
    }
    auto file = std::fopen(_filename.c_str(), "ab");
    if (file != nullptr) {
      std::fwrite(&value, sizeof(Meas), size_t(1), file);
      std::fclose(file);
      _writed++;
      return append_result(1, 0);
    } else {
      throw MAKE_EXCEPTION("aofile: append error.");
    }
  }

  append_result append(const Meas::MeasArray::const_iterator &begin,
                       const Meas::MeasArray::const_iterator &end) {
    TIMECODE_METRICS(ctmd, "append", "AOFile::append(ma)");
    assert(!_is_readonly);
    std::lock_guard<std::mutex> lock(_mutex);
    auto sz = std::distance(begin, end);
    if (is_full) {
      return append_result(0, sz);
    }
    auto file = std::fopen(_filename.c_str(), "ab");
    if (file != nullptr) {

      auto write_size = (sz + _writed) > _params.size ? (_params.size - _writed) : sz;
      std::fwrite(&(*begin), sizeof(Meas), write_size, file);
      std::fclose(file);
      _writed += write_size;
      return append_result(write_size, 0);
    } else {
      throw MAKE_EXCEPTION("aofile: append error.");
    }
  }

  append_result append(const Meas::MeasList::const_iterator &begin,
                       const Meas::MeasList::const_iterator &end) {
    TIMECODE_METRICS(ctmd, "append", "AOFile::append(ml)");
    assert(!_is_readonly);
    std::lock_guard<std::mutex> lock(_mutex);
    auto list_size = std::distance(begin, end);
    if (is_full) {
      return append_result(0, list_size);
    }
    auto file = std::fopen(_filename.c_str(), "ab");
    if (file != nullptr) {

      auto write_size =
          (list_size + _writed) > _params.size ? (_params.size - _writed) : list_size;
      Meas::MeasArray ma{begin, end};
      std::fwrite(ma.data(), sizeof(Meas), write_size, file);
      std::fclose(file);
      _writed += write_size;
      return append_result(write_size, 0);
    } else {
      throw MAKE_EXCEPTION("aofile: append error.");
    }
  }

  void foreach (const QueryInterval &q, IReaderClb * clbk) {
    TIMECODE_METRICS(ctmd, "foreach", "AOFile::foreach");
    std::lock_guard<std::mutex> lock(_mutex);

    auto file = std::fopen(_filename.c_str(), "rb");
    if (file == nullptr) {
      throw_open_error_exception();
    }

    while (1) {
      Meas val = Meas::empty();
      if (fread(&val, sizeof(Meas), size_t(1), file) == 0) {
        break;
      }
      if (val.inQuery(q.ids, q.flag, q.source, q.from, q.to)) {
        clbk->call(val);
      }
    }
    std::fclose(file);
  }


  Meas::Id2Meas readInTimePoint(const QueryTimePoint &q) {
    TIMECODE_METRICS(ctmd, "readInTimePoint", "AOFile::readInTimePoint");
    std::lock_guard<std::mutex> lock(_mutex);
    dariadb::IdSet readed_ids;
    dariadb::Meas::Id2Meas sub_res;

    auto file = std::fopen(_filename.c_str(), "rb");
    if (file == nullptr) {
      this->throw_open_error_exception();
    }
    while (1) {
      Meas val = Meas::empty();
      if (fread(&val, sizeof(Meas), size_t(1), file) == 0) {
        break;
      }
      if (val.inQuery(q.ids, q.flag, q.source) && (val.time <= q.time_point)) {
        replace_if_older(sub_res, val);
        readed_ids.insert(val.id);
      }
    }
    std::fclose(file);

    if (!q.ids.empty() && readed_ids.size() != q.ids.size()) {
      for (auto id : q.ids) {
        if (readed_ids.find(id) == readed_ids.end()) {
          auto e = Meas::empty(id);
          e.flag = Flags::_NO_DATA;
          e.time = q.time_point;
          sub_res[id] = e;
        }
      }
    }

    return sub_res;
  }

  void replace_if_older(dariadb::Meas::Id2Meas &s, const dariadb::Meas &m) const {
    auto fres = s.find(m.id);
    if (fres == s.end()) {
      s.insert(std::make_pair(m.id, m));
    } else {
      if (fres->second.time < m.time) {
        s.insert(std::make_pair(m.id, m));
      }
    }
  }

  Meas::Id2Meas currentValue(const IdArray &ids, const Flag &flag) {
    std::lock_guard<std::mutex> lock(_mutex);
    dariadb::Meas::Id2Meas sub_res;
    dariadb::IdSet readed_ids;
    auto file = std::fopen(_filename.c_str(), "rb");
    if (file == nullptr) {
      throw_open_error_exception();
    }
    while (1) {
      Meas val = Meas::empty();
      if (fread(&val, sizeof(Meas), size_t(1), file) == 0) {
        break;
      }
      if (val.inFlag(flag)) {
        replace_if_older(sub_res, val);
        readed_ids.insert(val.id);
      }
    }
    std::fclose(file);

    if (!ids.empty() && readed_ids.size() != ids.size()) {
      for (auto id : ids) {
        if (readed_ids.find(id) == readed_ids.end()) {
          auto e = Meas::empty(id);
          e.flag = Flags::_NO_DATA;
          e.time = dariadb::Time(0);
          sub_res[id] = e;
        }
      }
    }

    return sub_res;
  }

  dariadb::Time minTime() const {
    std::lock_guard<std::mutex> lock(_mutex);
    auto file = std::fopen(_filename.c_str(), "rb");
    if (file == nullptr) {
      throw_open_error_exception();
    }

    dariadb::Time result = dariadb::MAX_TIME;

    while (1) {
      Meas val = Meas::empty();
      if (fread(&val, sizeof(Meas), size_t(1), file) == 0) {
        break;
      }
      result = std::min(val.time, result);
    }
    std::fclose(file);
    return result;
  }

  dariadb::Time maxTime() const {
    auto file = std::fopen(_filename.c_str(), "rb");
    if (file == nullptr) {
      throw_open_error_exception();
    }

    dariadb::Time result = dariadb::MIN_TIME;

    while (1) {
      Meas val = Meas::empty();
      if (fread(&val, sizeof(Meas), size_t(1), file) == 0) {
        break;
      }
      result = std::max(val.time, result);
    }
    std::fclose(file);
    return result;
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult, dariadb::Time *maxResult) {
    TIMECODE_METRICS(ctmd, "minMaxTime", "AOFile::minMaxTime");
    auto file = std::fopen(_filename.c_str(), "rb");
    if (file == nullptr) {
      throw_open_error_exception();
    }

    *minResult = dariadb::MAX_TIME;
    *maxResult = dariadb::MIN_TIME;
    bool result = false;
    while (1) {
      Meas val = Meas::empty();
      if (fread(&val, sizeof(Meas), size_t(1), file) == 0) {
        break;
      }
      if (val.id == id) {
        result = true;
        *minResult = std::min(*minResult, val.time);
        *maxResult = std::max(*maxResult, val.time);
      }
    }
    std::fclose(file);
    return result;
  }

  void flush() { TIMECODE_METRICS(ctmd, "flush", "AOFile::flush"); }

  std::string filename() const { return _filename; }

  Meas::MeasArray readAll() {
    TIMECODE_METRICS(ctmd, "drop", "AOFile::drop");
    auto file = std::fopen(_filename.c_str(), "rb");
    if (file == nullptr) {
      throw_open_error_exception();
    }

    Meas::MeasArray ma(_params.size);
    size_t pos = 0;
    while (1) {
      Meas val = Meas::empty();
      if (fread(&val, sizeof(Meas), size_t(1), file) == 0) {
        break;
      }
      ma[pos] = val;
      pos++;
    }
    std::fclose(file);
    return ma;
  }

  void throw_open_error_exception() const {
    std::stringstream ss;
    ss << "aof: file open error " << _filename;
    auto aofs_manifest = Manifest::instance()->aof_list();
    ss << "Manifest:";
    for (auto f : aofs_manifest) {
      ss << f << std::endl;
    }
    auto aofs_exists = utils::fs::ls(_params.path, ".aof");
    for (auto f : aofs_exists) {
      ss << f << std::endl;
    }
    throw MAKE_EXCEPTION(ss.str());
  }

protected:
  AOFile::Params _params;
  std::string _filename;

  mutable std::mutex _mutex;
  bool _is_readonly;
  size_t _writed;
  bool is_full;
};

AOFile::~AOFile() {}

AOFile::AOFile(const Params &params) : _Impl(new AOFile::Private(params)) {}

AOFile::AOFile(const AOFile::Params &params, const std::string &fname, bool readonly)
    : _Impl(new AOFile::Private(params, fname, readonly)) {}

dariadb::Time AOFile::minTime() {
  return _Impl->minTime();
}

dariadb::Time AOFile::maxTime() {
  return _Impl->maxTime();
}

bool AOFile::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                        dariadb::Time *maxResult) {
  return _Impl->minMaxTime(id, minResult, maxResult);
}
void AOFile::flush() { // write all to storage;
  _Impl->flush();
}

append_result AOFile::append(const Meas &value) {
  return _Impl->append(value);
}
append_result AOFile::append(const Meas::MeasArray::const_iterator &begin,
                             const Meas::MeasArray::const_iterator &end) {
  return _Impl->append(begin, end);
}
append_result AOFile::append(const Meas::MeasList::const_iterator &begin,
                             const Meas::MeasList::const_iterator &end) {
  return _Impl->append(begin, end);
}

void AOFile::foreach (const QueryInterval &q, IReaderClb * clbk) {
  return _Impl->foreach (q, clbk);
}

Meas::Id2Meas AOFile::readInTimePoint(const QueryTimePoint &q) {
  return _Impl->readInTimePoint(q);
}

Meas::Id2Meas AOFile::currentValue(const IdArray &ids, const Flag &flag) {
  return _Impl->currentValue(ids, flag);
}

std::string AOFile::filename() const {
  return _Impl->filename();
}

Meas::MeasArray AOFile::readAll() {
  return _Impl->readAll();
}

size_t AOFile::writed(std::string fname) {
  TIMECODE_METRICS(ctmd, "read", "AOFile::writed");
  std::ifstream in(fname, std::ifstream::ate | std::ifstream::binary);
  return in.tellg() / sizeof(Meas);
}
