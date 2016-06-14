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
#include "../timeutil.h"
#include "../utils/crc.h"
#include "../utils/cz.h"
#include "../utils/fs.h"
#include "../utils/kmerge.h"
#include "../utils/utils.h"
#include "inner_readers.h"
#include "manifest.h"
#include <algorithm>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <cassert>
#include <cstring>
#include <future>
#include <limits>
#include <list>
#include <tuple>

const std::string LOG_MSG_PREFIX = "Capacitor: ";

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils;

#pragma pack(push, 1)
struct level_header {
  uint8_t lvl;
  uint64_t count;
  uint64_t pos;
  dariadb::Time _minTime;
  dariadb::Time _maxTime;
  uint32_t crc;
};
#pragma pack(pop)

struct FlaggedMeas {
  uint8_t drop_start : 1;
  uint8_t drop_end : 1;
  Meas value;

  bool operator==(const FlaggedMeas &other) const {
    return std::tie(drop_start, drop_end, value) ==
           std::tie(other.drop_start, other.drop_end, other.value);
  }
  bool operator!=(const FlaggedMeas &other) const { return !(*this == other); }
};

struct flagged_meas_time_compare_less {
  bool operator()(const FlaggedMeas &lhs, const FlaggedMeas &rhs) const {
    return meas_time_compare_less()(lhs.value, rhs.value);
  }
};

struct level {
  level_header *hdr;
  FlaggedMeas *begin;

  FlaggedMeas at(size_t _pos) const { return begin[_pos]; }
  FlaggedMeas &at(size_t _pos) { return begin[_pos]; }

  void push_back(const Meas &m) {
    assert(hdr->pos < hdr->count);
    begin[hdr->pos].value = m;
    ++hdr->pos;
    hdr->_minTime = std::min(hdr->_minTime, m.time);
    hdr->_maxTime = std::max(hdr->_maxTime, m.time);
  }

  uint32_t calc_checksum() {
    uint8_t *char_array = (uint8_t *)begin;
    return utils::crc32(char_array, sizeof(FlaggedMeas) * hdr->count);
  }

  void update_checksum() { hdr->crc = calc_checksum(); }

  bool check_checksum() { return hdr->crc != 0 && hdr->crc == calc_checksum(); }

  /// need for k-merge
  void push_back(const FlaggedMeas &m) {
    this->push_back(m.value);
    hdr->_minTime = std::min(hdr->_minTime, m.value.time);
    hdr->_maxTime = std::max(hdr->_maxTime, m.value.time);
  }

  FlaggedMeas back() const { return begin[hdr->pos - 1]; }

  void clear() {
    for (size_t i = 0; i < hdr->pos; ++i) {
      this->begin[i].drop_end = 0;
      this->begin[i].drop_start = 0;
    }
    hdr->crc = 0;
    hdr->pos = 0;
    hdr->_maxTime = dariadb::MIN_TIME;
    hdr->_minTime = dariadb::MAX_TIME;
  }

  bool empty() const { return hdr->pos == 0; }

  size_t size() const { return hdr->count; }
};

class Capacitor::Private {
public:
#pragma pack(push, 1)
  struct Header {
    bool is_dropped : 1;
    bool is_closed : 1;
    size_t B;
    size_t size;    // sizeof file in bytes
    size_t _size_B; // how many block (sizeof(B)) addeded.
    size_t levels_count;
    size_t _writed;
    size_t _memvalues_pos;
  };
#pragma pack(pop)
  Private(MeasWriter *stor, const Capacitor::Params &params)
      : _stor(stor), _params(params), mmap(nullptr), _size(0) {
    _maxTime = dariadb::MIN_TIME;
    _minTime = dariadb::MAX_TIME;
    open_or_create();
  }

  ~Private() {
    this->flush();
    if (mmap != nullptr) {
      _header->is_closed = true;
      mmap->close();
    }
  }

  void open_or_create() {
    if (!fs::path_exists(_params.path)) {
      create();
    } else {
      auto logs = Manifest::instance()->cola_list();
      if (logs.empty()) {
        create();
      } else {
        open(logs.front());
      }
    }
  }

  size_t one_block_size(size_t B) const { return sizeof(FlaggedMeas) * B; }

  size_t block_in_level(size_t lev_num) const { return (size_t(1) << lev_num); }

  size_t bytes_in_level(size_t B, size_t lvl) const {
    auto blocks_count = block_in_level(lvl);
    auto res = one_block_size(B) * blocks_count;
    return res;
  }

  uint64_t cap_size() const {
    uint64_t result = 0;

    result += sizeof(Header);
    result += _params.B * sizeof(FlaggedMeas); /// space to _memvalues

    auto prev_level_size = _params.B * sizeof(FlaggedMeas);
    // TODO shame!
    for (size_t lvl = 0; lvl < _params.max_levels; ++lvl) {
      auto cur_level_meases = bytes_in_level(_params.B, lvl); /// 2^lvl
      if (cur_level_meases < prev_level_size) {
        throw MAKE_EXCEPTION("size calculation error");
      }
      prev_level_size = cur_level_meases;
      result += sizeof(level_header) + cur_level_meases;
    }
    return result;
  }

  std::string file_name() { return fs::random_file_name(CAP_FILE_EXT); }

  void create() {
    fs::mkdir(_params.path);
    fs::mkdir(_params.path);
    auto sz = cap_size();
    auto fname = file_name();
    mmap = fs::MappedFile::touch(fs::append_path(_params.path, fname), sz);

    _header = reinterpret_cast<Header *>(mmap->data());
    _raw_data = reinterpret_cast<uint8_t *>(_header + sizeof(Header));
    _header->size = sz;
    _header->B = _params.B;
    _header->is_dropped = false;
    _header->levels_count = _params.max_levels;
    _header->is_closed = false;
    auto pos = _raw_data + _header->B * sizeof(FlaggedMeas); // move to levels position
    for (size_t lvl = 0; lvl < _header->levels_count; ++lvl) {
      auto it = reinterpret_cast<level_header *>(pos);
      it->lvl = uint8_t(lvl);
      it->count = block_in_level(lvl) * _params.B;
      it->pos = 0;
      it->_minTime = dariadb::MAX_TIME;
      it->_maxTime = dariadb::MIN_TIME;
      auto m = reinterpret_cast<FlaggedMeas *>(pos + sizeof(level_header));
      for (size_t i = 0; i < it->count; ++i) {
        std::memset(&m[i], 0, sizeof(FlaggedMeas));
      }
      pos += sizeof(level_header) + bytes_in_level(_header->B, lvl);
    }

    Manifest::instance()->cola_append(fname);
    load();
  }

  void load() {
    _levels.resize(_header->levels_count);
    _memvalues_size = _header->B;
    _memvalues = reinterpret_cast<FlaggedMeas *>(_raw_data);
    for (size_t i = 0; i < _header->_memvalues_pos; ++i) {
      _minTime = std::min(_minTime, _memvalues[i].value.time);
      _maxTime = std::max(_maxTime, _memvalues[i].value.time);
    }

    auto pos = _raw_data + _memvalues_size * sizeof(FlaggedMeas);

    for (size_t lvl = 0; lvl < _header->levels_count; ++lvl) {
      auto h = reinterpret_cast<level_header *>(pos);
      auto m = reinterpret_cast<FlaggedMeas *>(pos + sizeof(level_header));
      level new_l;
      new_l.begin = m;
      new_l.hdr = h;
      _levels[lvl] = new_l;
      pos += sizeof(level_header) + bytes_in_level(_header->B, lvl);
    }
  }

  void open(const std::string &fname) {
    auto aof_file = fs::append_path(_params.path, fname);
    mmap = fs::MappedFile::open(aof_file);

    _header = reinterpret_cast<Header *>(mmap->data());

    _raw_data = reinterpret_cast<uint8_t *>(_header + sizeof(Header));

    load();
    if (!_header->is_closed) {
      restore();
    }
    _header->is_closed = false;
  }

  void restore() {
    using dariadb::timeutil::to_string;

    logger_info(LOG_MSG_PREFIX << "restore after crash");
    Meas::MeasList readed;
    size_t dropped = 0;

    for (size_t i = 0; i < _header->levels_count; ++i) {
      auto current = &_levels[i];
      if (!current->check_checksum()) {
        logger_fatal(LOG_MSG_PREFIX << "level #" << i << " (cap: " << current->hdr->count
                                    << " size: " << current->hdr->pos << " time: ["
                                    << to_string(current->hdr->_minTime) << " : "
                                    << to_string(current->hdr->_maxTime) << "])"
                                    << " checksum error.");
        dropped += current->hdr->pos;
        _levels[i].clear();
      } else {
        for (size_t pos = 0; pos < current->hdr->pos; ++pos) {
          readed.push_back(current->begin[pos].value);
        }
      }
    }
    logger_info(LOG_MSG_PREFIX << "dropped " << dropped << " values.");
    if (!readed.empty()) {
      logger_info(LOG_MSG_PREFIX << "rewrite " << readed.size() << " values.");
      this->_header->_memvalues_pos = 0;
      this->_header->_writed = 0;
      for (auto &m : readed) {
        this->append(m);
      }
    }
  }

  append_result append_unsafe(const Meas &value) {
    if (_header->_memvalues_pos < _header->B) {
      return append_to_mem(value);
    } else {

      return append_to_levels(value);
    }
  }

  append_result append(const Meas &value) {
    boost::upgrade_lock<boost::shared_mutex> lock(_mutex);

    auto result = append_unsafe(value);
    // mmap->flush();
    return result;
  }

  append_result append_to_mem(const Meas &value) {
    _memvalues[_header->_memvalues_pos].value = value;

    _header->_memvalues_pos++;
    ++_header->_writed;
    _minTime = std::min(_minTime, value.time);
    _maxTime = std::max(_maxTime, value.time);

    return append_result(1, 0);
  }

  append_result append_to_levels(const Meas &value) {
    size_t outlvl = merge_levels();
    auto merge_target = _levels[outlvl];
    if (outlvl == (_header->levels_count - 1)) {
      auto target = &_levels[_header->levels_count - 1];
      _header->_size_B -= target->size() / _header->B;
      _header->_writed -= target->size();

      merge_target.clear();
      drop_future = std::async(std::launch::async, &Capacitor::Private::drop_one_level,
                               this, target);
    }
    return append_to_mem(value);
  }

  size_t calc_outlevel_num() {
    size_t new_items_count = _header->_size_B + 1;
    return dariadb::utils::ctz(~_size & new_items_count);
  }

  size_t merge_levels() {
    flagged_meas_time_compare_less flg_less_by_time;
    std::sort(_memvalues, _memvalues + _memvalues_size, flg_less_by_time);

    size_t outlvl = calc_outlevel_num();

    if (outlvl >= _header->levels_count) {
      assert(false);
      // drop_to_stor();
      return 0;
    }

    if (outlvl == (_header->levels_count - 1)) {
      if (drop_future.valid()) {
        drop_future.wait();
      }
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

    for (size_t i = 0; i < outlvl; ++i) {
      to_merge.push_back(&_levels[i]);
    }

    auto merge_target = _levels[outlvl];

    if (!merge_target.empty()) {
      logger_info(LOG_MSG_PREFIX << "merge target not empty.");
      merge_target.clear();
      logger_info(LOG_MSG_PREFIX << "clear done.");
    }
	dariadb::utils::k_merge(to_merge, merge_target, flg_less_by_time);

    merge_target.update_checksum();

    clean_merged_levels(outlvl);
    return outlvl;
  }

  void clean_merged_levels(size_t outlvl) {
    for (size_t i = 0; i < outlvl; ++i) {
      _levels[i].clear();
    }
    ++_header->_size_B;

    _header->_memvalues_pos = 0;

    // mmap->flush(0, 0);
  }

  std::future<void> drop_future;
  void drop_one_level(level *target) {
    if (_stor == nullptr) {
      return;
    }

    for (size_t i = 0; i < target->size(); ++i) {
      if (target->at(i).drop_end) {
        continue;
      }
      target->at(i).drop_start = 1;
      for (size_t j = 0; j < target->size(); ++j) {
        if (target->at(j).drop_end) {
          continue;
        }
        if (target->at(i).value.id == target->at(j).value.id) {
          _stor->append(target->at(j).value);
          target->at(j).drop_end = 1;
        }
      }
    }

    target->clear();
  }

  void flush_header() { mmap->flush(0, sizeof(Header)); }

  struct low_level_stor_pusher {
    MeasWriter *_stor;
    void push_back(FlaggedMeas &m) {
      if (m.drop_end) {
        return;
      }
      ++m.drop_start;
      _stor->append(m.value);
      ++m.drop_end;
    }
  };

  /*void drop_to_stor() {
     std::list<level *> to_merge;
     level tmp;
     level_header tmp_hdr;
     tmp.hdr = &tmp_hdr;
     tmp.hdr->lvl = 0;
     tmp.hdr->count = _memvalues_size;
     tmp.hdr->pos = _memvalues_size;
     tmp.begin = _memvalues;
     to_merge.push_back(&tmp);

     for (size_t i = 0; i < _levels.size(); ++i) {
       if (!_levels[i].empty()) {
         to_merge.push_back(&_levels[i]);
       }
     }
     low_level_stor_pusher merge_target;
     merge_target._stor = _stor;

     dariadb::utils::k_merge(to_merge, merge_target,
   flagged_meas_time_compare_less());
     for (size_t i = 0; i < _levels.size(); ++i) {
       _levels[i].clear();
     }
     _header->_memvalues_pos = 0;
     _header->_size_B = 0;
     _header->_writed = 0;
   }
 */
  Reader_ptr readInterval(const QueryInterval &q) {
    boost::shared_lock<boost::shared_mutex> lock(_mutex);
    TP_Reader *raw = new TP_Reader;
    std::map<dariadb::Id, std::set<Meas, meas_time_compare_less>> sub_result;

    if (q.from > this->minTime()) {
      auto tp_read_data = this->timePointValues(QueryTimePoint(q.ids, q.flag, q.from));
      for (auto kv : tp_read_data) {
        sub_result[kv.first].insert(kv.second);
      }
    }

    for (size_t i = 0; i < this->_levels.size(); ++i) {
      if (_levels[i].empty()) {
        continue;
      }
      if (!inInterval(q.from, q.to, _levels[i].hdr->_minTime) &&
          !inInterval(q.from, q.to, _levels[i].hdr->_maxTime) &&
          !inInterval(_levels[i].hdr->_minTime, _levels[i].hdr->_maxTime, q.from) &&
          !inInterval(_levels[i].hdr->_minTime, _levels[i].hdr->_maxTime, q.to)) {
        continue;
      }
      for (size_t j = 0; j < _levels[i].hdr->pos; ++j) {
        auto m = _levels[i].at(j);
        if (m.drop_end) {
          continue;
        }
        if (m.value.time > q.to) {
          break;
        }
        if (m.value.inQuery(q.ids, q.flag, q.from, q.to)) {
          sub_result[m.value.id].insert(m.value);
        }
      }
    }

    for (size_t j = 0; j < _header->_memvalues_pos; ++j) {
      auto m = _memvalues[j];
      if (m.drop_end) {
        continue;
      }
      if (m.value.inQuery(q.ids, q.flag, q.from, q.to)) {
        sub_result[m.value.id].insert(m.value);
      }
    }

    for (auto &kv : sub_result) {
      raw->_ids.push_back(kv.first);
      for (auto &m : kv.second) {
        raw->_values.push_back(m);
      }
    }
    raw->reset();
    return Reader_ptr(raw);
  }

  void insert_if_older(dariadb::Meas::Id2Meas &s, const dariadb::Meas &m) const {
    auto fres = s.find(m.id);
    if (fres == s.end()) {
      s.insert(std::make_pair(m.id, m));
    } else {
      if (fres->second.time < m.time) {
        s.insert(std::make_pair(m.id, m));
      }
    }
  }

  Reader_ptr readInTimePoint(const QueryTimePoint &q) {
    boost::shared_lock<boost::shared_mutex> lock(_mutex);
    TP_Reader *raw = new TP_Reader;
    dariadb::Meas::Id2Meas sub_res = timePointValues(q);

    for (auto kv : sub_res) {
      raw->_values.push_back(kv.second);
      raw->_ids.push_back(kv.first);
    }

    raw->reset();
    return Reader_ptr(raw);
  }

  Reader_ptr currentValue(const IdArray &ids, const Flag &flag) {
    boost::shared_lock<boost::shared_mutex> lock(_mutex);
    return readInTimePoint(QueryTimePoint(ids, flag, this->maxTime()));
  }

  dariadb::Time minTime() const {
    boost::shared_lock<boost::shared_mutex> lock(_mutex);
    dariadb::Time result = dariadb::MAX_TIME;
    for (size_t i = 0; i < _levels.size(); ++i) {
      if (!_levels[i].empty()) {
        result = std::min(result, _levels[i].hdr->_minTime);
      }
    }
    return std::min(result, _minTime);
  }

  dariadb::Time maxTime() const {
    boost::shared_lock<boost::shared_mutex> lock(_mutex);
    dariadb::Time result = dariadb::MIN_TIME;
    for (size_t i = 0; i < _levels.size(); ++i) {
      if (!_levels[i].empty()) {
        result = std::max(result, _levels[i].hdr->_maxTime);
      }
    }
    return std::max(result, _maxTime);
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult, dariadb::Time *maxResult) {
    boost::shared_lock<boost::shared_mutex> lock(_mutex);

    *minResult = dariadb::MAX_TIME;
    *maxResult = dariadb::MIN_TIME;
    bool result = false;
    for (size_t j = 0; j < _header->_memvalues_pos; ++j) {
      auto m = _memvalues[j];
      if (m.drop_end) {
        continue;
      }
      if (m.value.id == id) {
        *minResult = std::min(*minResult, m.value.time);
        *maxResult = std::max(*maxResult, m.value.time);
        result = true;
      }
    }

    for (size_t i = 0; i < this->_levels.size(); ++i) {
      if (_levels[i].empty()) {
        continue;
      }
      for (size_t j = 0; j < _levels[i].hdr->pos; ++j) {
        auto m = _levels[i].at(j);
        if (m.drop_end) {
          continue;
        }
        if (m.value.id == id) {
          *minResult = std::min(*minResult, m.value.time);
          *maxResult = std::max(*maxResult, m.value.time);
          result = true;
        }
      }
    }
    return result;
  }

  void flush() {
    boost::upgrade_lock<boost::shared_mutex> lock(_mutex);
    if (drop_future.valid()) {
      drop_future.wait();
    }
    mmap->flush();
  }

  size_t files_count() const { return Manifest::instance()->cola_list().size(); }

  size_t levels_count() const { return _levels.size(); }

  size_t size() const { return _header->_writed; }

  dariadb::Meas::Id2Meas timePointValues(const QueryTimePoint &q) {
    dariadb::IdSet readed_ids;
    dariadb::Meas::Id2Meas sub_res;

    if (inInterval(_minTime, _maxTime, q.time_point)) {
      for (size_t j = 0; j < _header->_memvalues_pos; ++j) {
        auto m = _memvalues[j];
        if (m.drop_end) {
          continue;
        }
        if (m.value.inQuery(q.ids, q.flag) && (m.value.time <= q.time_point)) {
          insert_if_older(sub_res, m.value);
          readed_ids.insert(m.value.id);
        }
      }
    }
    for (size_t i = 0; i < this->_levels.size(); ++i) {
      if (_levels[i].empty()) {
        continue;
      }
      for (size_t j = 0; j < _levels[i].hdr->pos; ++j) {
        if (!inInterval(_levels[i].hdr->_minTime, _levels[i].hdr->_maxTime,
                        q.time_point)) {
          continue;
        }
        auto m = _levels[i].at(j);
        if (m.drop_end) {
          continue;
        }

        if (m.value.time > q.time_point) {
          break;
        }
        if (m.value.inQuery(q.ids, q.flag) && (m.value.time <= q.time_point)) {
          insert_if_older(sub_res, m.value);
          readed_ids.insert(m.value.id);
        }
      }
    }

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

protected:
  MeasWriter *_stor;
  Capacitor::Params _params;

  dariadb::utils::fs::MappedFile::MapperFile_ptr mmap;
  Header *_header;
  uint8_t *_raw_data;
  std::vector<level> _levels;
  size_t _size;
  FlaggedMeas *_memvalues;
  size_t _memvalues_size;

  mutable boost::shared_mutex _mutex;
  dariadb::Time _minTime;
  dariadb::Time _maxTime;
};

Capacitor::~Capacitor() {}

Capacitor::Capacitor(MeasWriter *stor, const Params &params)
    : _Impl(new Capacitor::Private(stor, params)) {}

dariadb::Time Capacitor::minTime() {
  return _Impl->minTime();
}

dariadb::Time Capacitor::maxTime() {
  return _Impl->maxTime();
}

bool Capacitor::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                           dariadb::Time *maxResult) {
  return _Impl->minMaxTime(id, minResult, maxResult);
}
void Capacitor::flush() { // write all to storage;
  _Impl->flush();
}

append_result dariadb::storage::Capacitor::append(const Meas &value) {
  return _Impl->append(value);
}

// Reader_ptr dariadb::storage::Capacitor::readInterval(Time from, Time to) {
//  return _Impl->readInterval(from, to);
//}

// Reader_ptr dariadb::storage::Capacitor::readInTimePoint(Time time_point) {
//  return _Impl->readInTimePoint(time_point);
//}

Reader_ptr dariadb::storage::Capacitor::readInterval(const QueryInterval &q) {
  return _Impl->readInterval(q);
}

Reader_ptr dariadb::storage::Capacitor::readInTimePoint(const QueryTimePoint &q) {
  return _Impl->readInTimePoint(q);
}

Reader_ptr dariadb::storage::Capacitor::currentValue(const IdArray &ids,
                                                     const Flag &flag) {
  return _Impl->currentValue(ids, flag);
}

size_t dariadb::storage::Capacitor::files_count() const {
  return _Impl->files_count();
}

size_t dariadb::storage::Capacitor::levels_count() const {
  return _Impl->levels_count();
}

size_t dariadb::storage::Capacitor::size() const {
  return _Impl->size();
}
