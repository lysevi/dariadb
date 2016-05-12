/*
FileName:
GUID.aof
File struct:
   CapHeader| LevelHeader | Meas0 | Meas1|....| LevelHeader|Meas0 |Meas1...
Alg:
        1. Measurements save to COLA file struct
        2. When the append-only-file is fulle,
        it is converted to a sorted table (*.page) and a new log file is created
for future updates.
*/

#include "capacitor.h"
#include "../utils/cz.h"
#include "../utils/fs.h"
#include "../utils/utils.h"
#include <algorithm>
#include <cassert>
#include <limits>
#include <list>

using namespace dariadb;
using namespace dariadb::storage;

struct level_header {
  size_t lvl;
  size_t count;
  size_t pos;
};

struct level {
  level_header *hdr;
  Meas *begin;

  Meas at(size_t _pos) const { return begin[_pos]; }

  void push_back(const Meas &m) {
    begin[hdr->pos] = m;
    ++hdr->pos;
  }

  void clear() { hdr->pos = 0; }

  bool empty() const { return hdr->pos == 0; }
};

class CapReader : public storage::Reader {
public:
  bool isEnd() const override { return _values.empty(); }
  dariadb::IdArray getIds() const override {
    dariadb::IdSet res;
    for (auto v : _values) {
      res.insert(v.id);
    }
    return dariadb::IdArray(res.begin(), res.end());
  }
  void readNext(dariadb::storage::ReaderClb *clb) override {
    clb->call(_values.front());
    _values.pop_front();
  }
  Reader_ptr clone() const override { return nullptr; }
  void reset() override {}
  dariadb::Meas::MeasList _values;
  ~CapReader() {}
};

class Capacitor::Private {
public:
  struct meas_time_compare {
    bool operator()(const dariadb::Meas &lhs, const dariadb::Meas &rhs) const {
      return lhs.time < rhs.time;
    }
  };

  struct Header {
    bool is_dropped;
    size_t B;
    size_t size;    // sizeof file in bytes
    size_t _size_B; // how many block (sizeof(B)) addeded.
    size_t levels_count;
    size_t _writed;
  };

  Private(const BaseStorage_ptr stor, const Capacitor::Params &params)
      : _minTime(std::numeric_limits<dariadb::Time>::max()),
        _maxTime(std::numeric_limits<dariadb::Time>::min()), _stor(stor),
        _params(params), mmap(nullptr), _size(0) {
    open_or_create();
    _memvalues.resize(_header->B);
    _memvalues_pos = 0;
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
      auto logs = dariadb::utils::fs::ls(_params.path, CAP_FILE_EXT);
      if (logs.empty()) {
        create();
      } else {
        open(logs.front());
      }
    }
  }

  size_t one_block_size(size_t B) const { return sizeof(Meas) * B; }

  size_t block_in_level(size_t lev_num) const { return (size_t(1) << lev_num); }

  size_t meases_in_level(size_t B, size_t lvl) const {
    return one_block_size(B) * block_in_level(lvl);
  }

  uint64_t cap_size() const {
    uint64_t result = 0;

    for (size_t lvl = 0; lvl < _params.max_levels; ++lvl) {
      result += sizeof(level_header) + meases_in_level(_params.B, lvl); // 2^lvl
    }
    return result + sizeof(Header);
  }

  std::string file_name() {
    return dariadb::utils::fs::random_file_name(CAP_FILE_EXT);
  }

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
      it->pos = 0;
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

  void open(const std::string &fname) {
    mmap = utils::fs::MappedFile::open(fname);

    _header = reinterpret_cast<Header *>(mmap->data());
    _raw_data = reinterpret_cast<uint8_t *>(_header + sizeof(Header));

    load();
  }

  append_result append(const Meas &value) {
    if (_memvalues_pos < _header->B) {
      return append_to_mem(value);
    } else {
      return append_to_levels(value);
    }
  }

  append_result append_to_mem(const Meas &value) {
    _memvalues[_memvalues_pos] = value;
    _memvalues_pos++;
    ++_header->_writed;
    return append_result(1, 0);
  }

  append_result append_to_levels(const Meas &value) {
    meas_time_compare less_by_time;
    std::sort(_memvalues.begin(), _memvalues.end(), less_by_time);
    _memvalues_pos = 0;
    size_t new_items_count = _header->_size_B + 1;
    size_t outlvl = dariadb::utils::ctz(~_size & new_items_count);
    // std::cout<<"outlvl: "<<outlvl<<std::endl;
    if (outlvl >= _header->levels_count) {

      return append_result(0, 1);
    }

    std::list<level *> to_merge;
    level tmp;
    level_header tmp_hdr;
    tmp.hdr = &tmp_hdr;
    tmp.hdr->lvl = 0;
    tmp.hdr->count = _memvalues.size();
    tmp.hdr->pos = _memvalues.size();
    tmp.begin = _memvalues.data();
    to_merge.push_back(&tmp);

    for (size_t i = 1; i <= outlvl; ++i) {
      to_merge.push_back(&_levels[i - 1]);
    }

    auto merge_target = _levels[outlvl];

    { // merge
      auto vals_size = to_merge.size();
      std::list<size_t> poses;
      for (size_t i = 0; i < vals_size; ++i) {
        poses.push_back(0);
      }
      while (!to_merge.empty()) {
        vals_size = to_merge.size();
        // get cur max;
        auto with_max_index = poses.begin();
        auto max_val = to_merge.front()->at(*with_max_index);
        auto it = to_merge.begin();
        auto with_max_index_it = it;
        for (auto pos_it = poses.begin(); pos_it != poses.end(); ++pos_it) {
          if (!less_by_time(max_val, (*it)->at(*pos_it))) {
            with_max_index = pos_it;
            max_val = (*it)->at(*pos_it);
            with_max_index_it = it;
          }
          ++it;
        }

        auto val = (*with_max_index_it)->at(*with_max_index);
        merge_target.push_back(val);
        // remove ended in-list
        (*with_max_index)++;
        auto cur_src = (*with_max_index_it);
        if ((*with_max_index) >= cur_src->hdr->count) {
          poses.erase(with_max_index);
          to_merge.erase(with_max_index_it);
        }
      }
    }
    for (size_t i = 1; i <= outlvl; ++i) {
      _levels[i - 1].clear();
    }
    ++_header->_size_B;
    return append_to_mem(value);
  }
  Reader_ptr readInterval(Time from, Time to) {
    return readInterval(dariadb::IdArray{}, 0, from, to);
  }

  Reader_ptr readInTimePoint(Time time_point) {
    return readInTimePoint(dariadb::IdArray{}, 0, time_point);
  }

  virtual Reader_ptr readInterval(const IdArray &ids, Flag flag, Time from,
                                  Time to) {
    CapReader *raw = new CapReader;
    for (size_t j = 0; j < _memvalues_pos; ++j) {
      auto m = _memvalues[j];
      if(m.inQuery(ids,flag,from,to)) {
        raw->_values.push_back(_memvalues[j]);
      }
    }

    for (size_t i = 0; i < this->_levels.size(); ++i) {
      if (_levels[i].empty()) {
        continue;
      }
      for (size_t j = 0; j < _levels[i].hdr->pos; ++j) {
        auto m = _levels[i].at(j);
		if (m.time > to) {
			break;
		}
		if (m.inQuery(ids, flag, from, to)) {
          raw->_values.push_back(m);
        }
      }
    }
    return Reader_ptr(raw);
  }

  virtual Reader_ptr readInTimePoint(const IdArray &ids, Flag flag,
                                     Time time_point) {
    return nullptr;
  }

  Reader_ptr currentValue(const IdArray &ids, const Flag &flag) {
    return nullptr;
  }
  dariadb::Time minTime() const { return _minTime; }
  dariadb::Time maxTime() const { return _maxTime; }

  void flush() {}

  size_t in_queue_size() const { return 0; }

  size_t levels_count() const { return _levels.size(); }

  size_t size() const { return _header->_writed; }

protected:
  dariadb::Time _minTime;
  dariadb::Time _maxTime;
  BaseStorage_ptr _stor;
  Capacitor::Params _params;

  dariadb::utils::fs::MappedFile::MapperFile_ptr mmap;
  Header *_header;
  uint8_t *_raw_data;
  std::vector<level> _levels;
  size_t _size;
  std::vector<Meas> _memvalues;
  size_t _memvalues_pos;
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

void Capacitor::flush() { // write all to storage;
  _Impl->flush();
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

Reader_ptr dariadb::storage::Capacitor::currentValue(const IdArray &ids,
                                                     const Flag &flag) {
  return _Impl->currentValue(ids, flag);
}

size_t dariadb::storage::Capacitor::in_queue_size() const {
  return _Impl->in_queue_size();
}

size_t dariadb::storage::Capacitor::levels_count() const {
  return _Impl->levels_count();
}

size_t dariadb::storage::Capacitor::size() const {
  return _Impl->size();
}
