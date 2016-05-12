/*
FileName:
GUID.aof
File struct:
   CapHeader|memvalues(sizeof(B)*sizeof(Meas)| LevelHeader | Meas0 | Meas1|....|
LevelHeader|Meas0 |Meas1...
Alg:
        1. Measurements save to COLA file struct
        2. When the append-only-file is fulle,
        it is converted to a sorted table (*.page) and a new log file is created
for future updates.
*/

#include "capacitor.h"
#include "../flags.h"
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
  CapReader() { _values_iterator = this->_values.end(); }

  bool isEnd() const override { return _values_iterator == _values.end(); }

  dariadb::IdArray getIds() const override {
    dariadb::IdSet res;
    for (auto v : _values) {
      res.insert(v.id);
    }
    return dariadb::IdArray(res.begin(), res.end());
  }

  void readNext(dariadb::storage::ReaderClb *clb) override {
    if (_values_iterator != _values.end()) {
      clb->call(*_values_iterator);
      ++_values_iterator;
      return;
    }
  }

  Reader_ptr clone() const override {
    CapReader *raw = new CapReader;
    raw->_values = _values;
    raw->reset();
    return Reader_ptr(raw);
  }

  void reset() override { _values_iterator = _values.begin(); }

  ~CapReader() {}

  dariadb::Meas::MeasList _values;
  dariadb::Meas::MeasList::iterator _values_iterator;
};

class Capacitor::Private {
public:
  struct Header {
    bool is_dropped;
    size_t B;
    size_t size;    // sizeof file in bytes
    size_t _size_B; // how many block (sizeof(B)) addeded.
    size_t levels_count;
    size_t _writed;
    size_t _memvalues_pos;
  };

  Private(const MeasStorage_ptr stor, const Capacitor::Params &params)
      : _minTime(std::numeric_limits<dariadb::Time>::max()),
        _maxTime(std::numeric_limits<dariadb::Time>::min()), _stor(stor),
        _params(params), mmap(nullptr), _size(0) {
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

    result += _params.B * sizeof(Meas); // space to _memvalues
                                        // TODO shame!
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

    auto pos = _raw_data + _header->B * sizeof(Meas); // move to levels position
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
    _memvalues_size = _header->B;
    _memvalues = reinterpret_cast<Meas *>(_raw_data);

    auto pos = _raw_data + _memvalues_size * sizeof(Meas);

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
    if (_header->_memvalues_pos < _header->B) {
      return append_to_mem(value);
    } else {
      return append_to_levels(value);
    }
  }

  append_result append_to_mem(const Meas &value) {
    _memvalues[_header->_memvalues_pos] = value;
    _header->_memvalues_pos++;
    ++_header->_writed;
    this->_minTime = std::min(this->_minTime, value.time);
    this->_maxTime = std::max(this->_maxTime, value.time);
    return append_result(1, 0);
  }

  append_result append_to_levels(const Meas &value) {
    meas_time_compare less_by_time;
    std::sort(_memvalues, _memvalues + _memvalues_size, less_by_time);
    _header->_memvalues_pos = 0;
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
    tmp.hdr->count = _memvalues_size;
    tmp.hdr->pos = _memvalues_size;
    tmp.begin = _memvalues;
    to_merge.push_back(&tmp);

    for (size_t i = 1; i <= outlvl; ++i) {
      to_merge.push_back(&_levels[i - 1]);
    }

    auto merge_target = _levels[outlvl];

    { // TODO refact.
      // merge
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
    std::map<dariadb::Id, std::set<Meas, meas_time_compare>> sub_result;

    if (from > this->_minTime) {
      auto tp_read_data = this->timePointValues(ids, flag, from);
      for (auto kv : tp_read_data) {
        sub_result[kv.first].insert(kv.second);
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
          sub_result[m.id].insert(m);
        }
      }
    }

    for (size_t j = 0; j < _header->_memvalues_pos; ++j) {
      auto m = _memvalues[j];
      if (m.inQuery(ids, flag, from, to)) {
        sub_result[m.id].insert(m);
      }
    }

    for (auto &kv : sub_result) {
      for (auto &m : kv.second) {
        raw->_values.push_back(m);
      }
    }
    raw->reset();
    return Reader_ptr(raw);
  }

  void insert_if_older(dariadb::Meas::Id2Meas &s,
                       const dariadb::Meas &m) const {
    auto fres = s.find(m.id);
    if (fres == s.end()) {
      s.insert(std::make_pair(m.id, m));
    } else {
      if (fres->second.time < m.time) {
        s.insert(std::make_pair(m.id, m));
      }
    }
  }

  virtual Reader_ptr readInTimePoint(const IdArray &ids, Flag flag,
                                     Time time_point) {
    CapReader *raw = new CapReader;
    dariadb::Meas::Id2Meas sub_res = timePointValues(ids, flag, time_point);

    for (auto kv : sub_res) {
      raw->_values.push_back(kv.second);
    }

    raw->reset();
    return Reader_ptr(raw);
  }

  Reader_ptr currentValue(const IdArray &ids, const Flag &flag) {
    return readInTimePoint(ids, flag, this->maxTime());
  }

  dariadb::Time minTime() const { return _minTime; }
  dariadb::Time maxTime() const { return _maxTime; }

  void flush() {}

  size_t in_queue_size() const { return 0; }

  size_t levels_count() const { return _levels.size(); }

  size_t size() const { return _header->_writed; }

  dariadb::Meas::Id2Meas timePointValues(const IdArray &ids, Flag flag,
                                         Time time_point) {
    dariadb::IdSet readed_ids;
    dariadb::IdSet unreaded_ids;
    dariadb::Meas::Id2Meas sub_res;

    for (size_t j = 0; j < _header->_memvalues_pos; ++j) {
      auto m = _memvalues[j];
      if (m.inQuery(ids, flag) && (m.time <= time_point)) {
        insert_if_older(sub_res, m);
        readed_ids.insert(m.id);
      } else {
        unreaded_ids.insert(m.id);
      }
    }

    for (size_t i = 0; i < this->_levels.size(); ++i) {
      if (_levels[i].empty()) {
        continue;
      }
      for (size_t j = 0; j < _levels[i].hdr->pos; ++j) {
        auto m = _levels[i].at(j);
        if (m.time > time_point) {
          break;
        }
        if (m.inQuery(ids, flag) && (m.time <= time_point)) {
          insert_if_older(sub_res, m);
          readed_ids.insert(m.id);
        } else {
          unreaded_ids.insert(m.id);
        }
      }
    }

    if (!ids.empty() && readed_ids.size() != ids.size()) {
      for (auto id : ids) {
        if (readed_ids.find(id) == readed_ids.end()) {
          auto e = Meas::empty(id);
          e.flag = Flags::_NO_DATA;
          e.time = time_point;
          sub_res[id] = e;
        }
      }
    }

    if (ids.empty()) {
      for (auto id : unreaded_ids) {
        if (readed_ids.find(id) == readed_ids.end()) {
          auto e = Meas::empty(id);
          e.flag = Flags::_NO_DATA;
          e.time = time_point;
          sub_res[id] = e;
        }
      }
    }
    return sub_res;
  }

protected:
  dariadb::Time _minTime;
  dariadb::Time _maxTime;
  MeasStorage_ptr _stor;
  Capacitor::Params _params;

  dariadb::utils::fs::MappedFile::MapperFile_ptr mmap;
  Header *_header;
  uint8_t *_raw_data;
  std::vector<level> _levels;
  size_t _size;
  Meas *_memvalues;
  size_t _memvalues_size;
};

Capacitor::~Capacitor() {}

Capacitor::Capacitor(const MeasStorage_ptr stor, const Params &params)
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
