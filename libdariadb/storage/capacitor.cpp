#include "capacitor.h"
#include "../timeutil.h"
#include "../utils/fs.h"
#include "../utils/locker.h"
#include "../utils/utils.h"
#include <algorithm>
#include <cassert>
#include <limits>
#include <list>
#include <map>
#include <unordered_map>
#include <utility>

using namespace dariadb;
using namespace dariadb::storage;

struct level_header {
  size_t lvl;
  size_t count;
};

struct level {
  level_header *hdr;
  Meas *begin;
};

class Capacitor::Private {
public:
  struct Header {
    bool is_dropped;
    size_t B;
    size_t size;
    size_t levels_count;
    size_t _writed;
  };

  Private(const BaseStorage_ptr stor, const Capacitor::Params &params)
      : _minTime(std::numeric_limits<dariadb::Time>::max()),
        _maxTime(std::numeric_limits<dariadb::Time>::min()), _stor(stor),
        _params(params), mmap(nullptr) {
    open_or_create();
  }

  ~Private() {
    if (mmap != nullptr) {
      mmap->close();
    }
  }

  void open_or_create() {
    if (!dariadb::utils::fs::path_exists(_params.path)) {
      create();
    } else {
      open();
    }
  }

  size_t one_block_size(size_t B) const { return sizeof(Meas) * B; }

  size_t block_in_level(size_t lev_num) const { return (1 << lev_num); }

  size_t meases_in_level(size_t B, size_t lvl) const {
    return one_block_size(B) * block_in_level(lvl);
  }

  uint64_t cap_size() const {
    uint64_t result = 0;
    auto block_sz = one_block_size(_params.B);
    for (size_t lvl = 0; lvl < _params.max_levels; ++lvl) {
      result += sizeof(level_header) + meases_in_level(_params.B, lvl); // 2^lvl
    }
    return result + sizeof(Header);
  }

  std::string file_name() { return "1" + CAP_FILE_EXT; }

  void create() {
    dariadb::utils::fs::mkdir(_params.path);
    utils::fs::mkdir(_params.path);
    auto sz = cap_size();
    mmap = utils::fs::MappedFile::touch(
        utils::fs::append_path(_params.path, file_name()), sz);

    _header = reinterpret_cast<Header *>(mmap->data());
    _raw_data = reinterpret_cast<uint8_t *>(_header + sizeof(Header));
    _header->size = sz;
    _header->B = _params.B;
    _header->is_dropped = false;
    _header->levels_count = _params.max_levels;

    auto pos = _raw_data;
    for (size_t lvl = 0; lvl < _header->levels_count; ++lvl) {
      auto it = reinterpret_cast<level_header *>(pos);
      it->lvl = lvl;
      it->count = block_in_level(lvl) * _params.B;
      pos += sizeof(level_header) + meases_in_level(_header->B, lvl);
    }
    load();
  }

  void load() {
    _levels.resize(_header->levels_count);
    auto pos = _raw_data;
    for (size_t lvl = 0; lvl < _header->levels_count; ++lvl) {
      auto h = reinterpret_cast<level_header *>(pos);
      auto m = reinterpret_cast<Meas *>(pos + sizeof(level_header));
      level new_l;
      new_l.begin = m;
      new_l.hdr = h;
      _levels[lvl] = new_l;
      pos += sizeof(level_header) + meases_in_level(_header->B, lvl);
    }
  }

  void open() {
    mmap = utils::fs::MappedFile::open(
        utils::fs::append_path(_params.path, file_name()));

    _header = reinterpret_cast<Header *>(mmap->data());
    _raw_data = reinterpret_cast<uint8_t *>(_header + sizeof(Header));

    load();
  }

  append_result append(const Meas &value) { return append_result(0, 0); }

  Reader_ptr readInterval(Time from, Time to) { return nullptr; }
  virtual Reader_ptr readInTimePoint(Time time_point) { return nullptr; }

  virtual Reader_ptr readInterval(const IdArray &ids, Flag flag, Time from,
                                  Time to) {
    return nullptr;
  }

  virtual Reader_ptr readInTimePoint(const IdArray &ids, Flag flag,
                                     Time time_point) {
    return nullptr;
  }

  dariadb::Time minTime() const { return _minTime; }
  dariadb::Time maxTime() const { return _maxTime; }

  bool flush() { return false; }

  size_t in_queue_size() const { return 0; }

  size_t levels_count() const { return _levels.size(); }

protected:
  dariadb::Time _minTime;
  dariadb::Time _maxTime;
  BaseStorage_ptr _stor;
  Capacitor::Params _params;

  dariadb::utils::fs::MappedFile::MapperFile_ptr mmap;
  Header *_header;
  uint8_t *_raw_data;
  std::vector<level> _levels;
};

Capacitor::~Capacitor() {}

Capacitor::Capacitor(const BaseStorage_ptr stor, const Params &params)
    : _Impl(new Capacitor::Private(stor, params)) {}

dariadb::Time Capacitor::minTime() {
  return _Impl->minTime();
}

dariadb::Time Capacitor::maxTime() {
  return _Impl->maxTime();
}

bool Capacitor::flush() { // write all to storage;
  return _Impl->flush();
}

append_result dariadb::storage::Capacitor::append(const Meas &value) {
  return _Impl->append(value);
}

Reader_ptr dariadb::storage::Capacitor::readInterval(Time from, Time to) {
  return _Impl->readInterval(from, to);
}

Reader_ptr dariadb::storage::Capacitor::readInTimePoint(Time time_point) {
  return _Impl->readInTimePoint(time_point);
}

Reader_ptr dariadb::storage::Capacitor::readInterval(const IdArray &ids,
                                                     Flag flag, Time from,
                                                     Time to) {
  return _Impl->readInterval(ids, flag, from, to);
}

Reader_ptr dariadb::storage::Capacitor::readInTimePoint(const IdArray &ids,
                                                        Flag flag,
                                                        Time time_point) {
  return _Impl->readInTimePoint(ids, flag, time_point);
}

size_t dariadb::storage::Capacitor::in_queue_size() const {
  return _Impl->in_queue_size();
}

size_t dariadb::storage::Capacitor::levels_count() const {
  return _Impl->levels_count();
}