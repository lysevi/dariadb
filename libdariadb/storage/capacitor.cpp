/*
FileName:
GUID.cap
Measurements saves to COLA file struct
File struct:
   CapHeader
   |LevelHeader0| LevelHeader1| ...
   | memvalues(sizeof(B)*sizeof(Meas))
   | Meas0_0 | Meas0_1|....
   |Meas1_0 |Meas1_1...
*/

#include "capacitor.h"
#include "../flags.h"
#include "../timeutil.h"
#include "../utils/crc.h"
#include "../utils/cz.h"
#include "../utils/fs.h"
#include "../utils/kmerge.h"
#include "../utils/metrics.h"
#include "../utils/utils.h"
#include "callbacks.h"
#include "manifest.h"
#include <algorithm>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <cassert>
#include <cstring>
#include <fstream>
#include <future>
#include <iterator>
#include <limits>
#include <list>
#include <tuple>

const std::string LOG_MSG_PREFIX = "fsck: capacitor ";

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils;

#pragma pack(push, 1)
struct level_header {
  uint8_t lvl;            // level num
  uint64_t count;         // values count
  uint64_t pos;           // values addeded
  dariadb::Time _minTime; // minMax time
  dariadb::Time _maxTime;
  uint32_t crc;      // checksum
  uint64_t id_bloom; // blooms
  uint64_t flag_bloom;
};
#pragma pack(pop)

struct level {
  level_header *hdr;
  Meas *begin;

  Meas at(size_t _pos) const { return begin[_pos]; }
  Meas &at(size_t _pos) { return begin[_pos]; }

  void push_back(const Meas &m) {
    assert(hdr->pos < hdr->count);
    begin[hdr->pos] = m;
    ++hdr->pos;
    hdr->_minTime = std::min(hdr->_minTime, m.time);
    hdr->_maxTime = std::max(hdr->_maxTime, m.time);
    hdr->id_bloom = bloom_add(hdr->id_bloom, m.id);
    hdr->flag_bloom = bloom_add(hdr->flag_bloom, m.flag);
  }

  void update_header() {
    for (size_t i = 0; i < hdr->pos; ++i) {
      auto m = begin[i];
      hdr->_minTime = std::min(hdr->_minTime, m.time);
      hdr->_maxTime = std::max(hdr->_maxTime, m.time);
      hdr->id_bloom = bloom_add(hdr->id_bloom, m.id);
      hdr->flag_bloom = bloom_add(hdr->flag_bloom, m.flag);
    }
  }

  uint32_t calc_checksum() {
    uint8_t *char_array = (uint8_t *)begin;
    return utils::crc32(char_array, sizeof(Meas) * hdr->count);
  }

  void update_checksum() { hdr->crc = calc_checksum(); }

  bool check_checksum() { return hdr->crc != 0 && hdr->crc == calc_checksum(); }

  Meas back() const { return begin[hdr->pos - 1]; }

  void clear() {
    hdr->crc = 0;
    hdr->pos = 0;
    hdr->_maxTime = dariadb::MIN_TIME;
    hdr->_minTime = dariadb::MAX_TIME;
  }

  bool empty() const { return hdr->pos == 0; }

  size_t size() const { return hdr->count; }

  bool check_id(const dariadb::Id id) const { return bloom_check(hdr->id_bloom, id); }

  bool check_id(const dariadb::IdArray &ids) const {
    for (auto id : ids) {
      if (check_id(id)) {
        return true;
      }
    }
    return false;
  }

  bool check_flag(const dariadb::Flag flag) const {
    return bloom_check(hdr->flag_bloom, flag);
  }
};

class Capacitor::Private {
public:
  Private(const Capacitor::Params &params, const std::string &fname, bool readonly)
      : _params(params), mmap(nullptr), _size(0) {
    _is_readonly = readonly;
    if (utils::fs::path_exists(fname)) {
      open(fname);
    } else {
      create(fname);
    }
  }

  ~Private() {
    this->flush();
    if (!_is_readonly) {
      if (mmap != nullptr) {
        _header->is_closed = true;
        _header->is_open_to_write = false;
        mmap->close();
      }
    }
    mmap = nullptr;
  }

  void close() { _header->is_full = true; }

  size_t one_block_size(size_t B) const { return sizeof(Meas) * B; }

  size_t block_in_level(size_t lev_num) const { return (size_t(1) << lev_num); }

  size_t bytes_in_level(size_t B, size_t lvl) const {
    auto blocks_count = block_in_level(lvl);
    auto res = one_block_size(B) * blocks_count;
    return res;
  }

  uint64_t cap_size() const {
    uint64_t result = 0;

    result += sizeof(Header);
    result += _params.B * sizeof(Meas); /// add space to _memvalues

    auto prev_level_size = _params.B * sizeof(Meas);

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

  void create(std::string fname) {
    TIMECODE_METRICS(ctmd, "create", "Capacitor::create");
    auto sz = cap_size();
    mmap = fs::MappedFile::touch(fs::append_path(_params.path, fname), sz);

    _header = reinterpret_cast<Header *>(mmap->data());
    _raw_data = reinterpret_cast<uint8_t *>(mmap->data() + sizeof(Header));
    _header->size = sz;
    _header->B = _params.B;
    _header->is_dropped = false;
    _header->levels_count = _params.max_levels;
    _header->max_values_count = this->_params.measurements_count();
    _header->is_closed = false;
    _header->is_open_to_write = true;
    _header->minTime = dariadb::MAX_TIME;
    _header->maxTime = dariadb::MIN_TIME;
    _header->id_bloom = bloom_empty<dariadb::Id>();
    _header->flag_bloom = bloom_empty<dariadb::Flag>();

    level_header *headers_pos = reinterpret_cast<level_header *>(_raw_data);

    Meas *pos_after_headers =
        reinterpret_cast<Meas *>(headers_pos + _header->levels_count);

    Meas *pos = (pos_after_headers + _header->B);
    for (size_t lvl = 0; lvl < _header->levels_count; ++lvl) {
      auto it = &headers_pos[lvl];
      it->lvl = uint8_t(lvl);
      it->count = block_in_level(lvl) * _params.B;
      it->pos = 0;
      it->_minTime = dariadb::MAX_TIME;
      it->_maxTime = dariadb::MIN_TIME;
      it->id_bloom = bloom_empty<dariadb::Id>();
      it->flag_bloom = bloom_empty<dariadb::Flag>();
      auto m = reinterpret_cast<Meas *>(pos);
      for (size_t i = 0; i < it->count; ++i) {
        assert(size_t((uint8_t *)&m[i] - mmap->data()) < sz);
        std::memset(&m[i], 0, sizeof(Meas));
      }
      pos += it->count; // bytes_in_level(_header->B, lvl);
    }

    Manifest::instance()->cola_append(fname);
    load();
  }

  void load() {
    TIMECODE_METRICS(ctmd, "open", "Capacitor::load");
    _levels.resize(_header->levels_count);
    _memvalues_size = _header->B;

    level_header *headers_pos = reinterpret_cast<level_header *>(_raw_data);

    _memvalues = reinterpret_cast<Meas *>(headers_pos + _header->levels_count);

    Meas *pos = (_memvalues + _header->B);

    for (size_t lvl = 0; lvl < _header->levels_count; ++lvl) {
      auto lvl_hdr = &headers_pos[lvl];
      auto m = reinterpret_cast<Meas *>(pos);
      level new_l;
      new_l.begin = m;
      new_l.hdr = lvl_hdr;
      _levels[lvl] = new_l;
      pos += lvl_hdr->count;
    }
  }

  void open(const std::string &fname) {
    mmap = fs::MappedFile::open(fname, _is_readonly);

    _header = reinterpret_cast<Header *>(mmap->data());

    _raw_data = reinterpret_cast<uint8_t *>(mmap->data() + sizeof(Header));

    load();
  }

  void fsck() {
    using dariadb::timeutil::to_string;

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
          readed.push_back(current->begin[pos]);
        }
      }
    }
    if (dropped != 0) {
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
  }

  append_result append_unsafe(const Meas &value) {
    if (_header->_memvalues_pos < _header->B) {
      return append_to_mem(value);
    } else {
      return append_to_levels(value);
    }
  }

  append_result append(const Meas &value) {
    TIMECODE_METRICS(ctmd, "append", "Capacitor::append");
    assert(!_is_readonly);
    boost::upgrade_lock<boost::shared_mutex> lock(_mutex);

    auto result = append_unsafe(value);
    return result;
  }

  /// bulk loading. values should ve sorted by time.
  append_result bulk(const Meas::MeasArray::const_iterator &begin,
                     const Meas::MeasArray::const_iterator &end) {
    level_header *headers_pos = reinterpret_cast<level_header *>(_raw_data);
    Meas *pos_after_headers =
        reinterpret_cast<Meas *>(headers_pos + _header->levels_count);

    auto src_size = std::distance(begin, end);
    auto dst_begin = pos_after_headers;

    _header->_memvalues_pos = _header->B;
    _header->_writed = src_size;
    _header->minTime = begin->time;
    _header->maxTime = (begin + src_size - 1)->time;

    auto src_it = begin;
    auto it = dst_begin;
    for (; src_it != end; ++it, ++src_it) {
      *it = *src_it;
      auto value = *it;

      _header->id_bloom = bloom_add(_header->id_bloom, value.id);
      _header->flag_bloom = bloom_add(_header->flag_bloom, value.flag);
    }
    for (auto &lvl : this->_levels) {
      lvl.hdr->pos = lvl.hdr->count;
      lvl.update_header();
      lvl.update_checksum();
    }
    return append_result(src_size, 0);
  }

  append_result append(const Meas::MeasArray::const_iterator &begin,
                       const Meas::MeasArray::const_iterator &end) {
    TIMECODE_METRICS(ctmd, "append", "Capacitor::append");
    assert(!_is_readonly);
    boost::upgrade_lock<boost::shared_mutex> lock(_mutex);

    if (size_t(std::distance(begin, end)) == (_header->max_values_count)) {
      return bulk(begin, end);
    } else {
      append_result result;
      for (auto it = begin; it != end; ++it) {
        auto v = *it;
        auto local_res = append_unsafe(v);
        if (local_res.writed == 0) {
          return result;
        }
        result = result + local_res;
      }

      return result;
    }
  }

  /// add values to zero level
  append_result append_to_mem(const Meas &value) {
    _memvalues[_header->_memvalues_pos] = value;

    _header->_memvalues_pos++;
    ++_header->_writed;
    _header->minTime = std::min(_header->minTime, value.time);
    _header->maxTime = std::max(_header->maxTime, value.time);
    _header->id_bloom = bloom_add(_header->id_bloom, value.id);
    _header->flag_bloom = bloom_add(_header->flag_bloom, value.flag);
    return append_result(1, 0);
  }

  append_result append_to_levels(const Meas &value) {
    merge_levels();
    if (this->_header->is_full) {
      return append_result(0, 1);
    }
    return append_to_mem(value);
  }

  uint8_t calc_outlevel_num() {
    size_t new_items_count = _header->_size_B + 1;
    return dariadb::utils::ctz(~_size & new_items_count);
  }

  size_t merge_levels() {
    TIMECODE_METRICS(ctmd, "write", "Capacitor::merge_levels");
    meas_time_compare_less flg_less_by_time;
    std::sort(_memvalues, _memvalues + _memvalues_size, flg_less_by_time);

    uint8_t outlvl = calc_outlevel_num();

    if (outlvl >= _header->levels_count) {
      this->_header->is_full = true;
      return outlvl;
    }

    auto merge_target = _levels[outlvl];

    if (outlvl == (_header->levels_count - 1)) {
      if (!merge_target.empty()) {
        this->_header->is_full = true;
        return outlvl;
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

    if (!merge_target.empty()) {
      throw MAKE_EXCEPTION("merge target not empty.");
    }

    bool is_sorted = true;
    for (auto it = to_merge.begin(); it != to_merge.end(); ++it) {
      auto next = std::next(it);
      if (next == to_merge.end()) {
        break;
      }
      if (((*it)->back().time) < (*next)->begin->time) {
        is_sorted = false;
        break;
      }
    }
    // if values in levels sorted, merge dont needed.
    if (is_sorted) {
      auto begin = to_merge.front()->begin;
      auto end = to_merge.back()->begin + to_merge.back()->hdr->pos;
      auto sz = (end - begin);
      memcpy(merge_target.begin, begin, sz * sizeof(Meas));
      merge_target.hdr->pos = merge_target.hdr->count;
      merge_target.update_header();
    } else {
      dariadb::utils::k_merge(to_merge, merge_target, flg_less_by_time);
    }
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
  }

  void flush_header() { mmap->flush(0, sizeof(Header)); }

  struct low_level_stor_pusher {
    IMeasWriter *_stor;
    void push_back(Meas &m) {
      _stor->append(m);
      _back = m;
    }

    size_t size() const { return 0; }
    Meas _back;
    Meas back() { return _back; }
  };

  /// values dropped to down-level storage. grouped by id.
  void drop_to_stor(IMeasWriter *stor) {
    TIMECODE_METRICS(ctmd, "drop", "Capacitor::drop_to_stor");

    level_header *headers_pos = reinterpret_cast<level_header *>(_raw_data);

    Meas *pos_after_headers =
        reinterpret_cast<Meas *>(headers_pos + _header->levels_count);

    auto begin = pos_after_headers;
    auto end = (Meas *)(begin + _header->_writed);
    size_t count = std::distance(begin, end);

    size_t pos = 0;

    std::sort(begin, end, meas_time_compare_less());
    dariadb::IdSet dropped;
    std::vector<bool> visited(count);

    size_t i = 0;
    for (auto it = begin; it != end; ++it, ++i) {
      if (visited[i]) {
        continue;
      }
      if (dropped.find(it->id) != dropped.end()) {
        continue;
      }
      visited[i] = true;
      stor->append(*it);
      pos = 0;
      for (auto sub_it = begin; sub_it != end; ++sub_it, ++pos) {
        if (visited[pos]) {
          continue;
        }
        if ((sub_it->id == it->id)) {
          stor->append(*sub_it);
          visited[pos] = true;
        }
      }
      dropped.insert(it->id);
    }
    _header->is_dropped = true;
  }

  void foreach (const QueryInterval &q, IReaderClb * clbk) {
    TIMECODE_METRICS(ctmd, "foreach", "Capacitor::foreach");
    boost::shared_lock<boost::shared_mutex> lock(_mutex);

    bool id_exists = _header->check_id(q.ids);
    bool flag_exists = false;
    if (_header->check_flag(q.flag)) {
      flag_exists = true;
    }
    if (!id_exists || !flag_exists) {
      return;
    }

    for (size_t i = 0; i < this->_levels.size(); ++i) {
      if (_levels[i].empty()) {
        continue;
      }
      id_exists = _levels[i].check_id(q.ids);
      if (!id_exists) {
        continue;
      }
      if (!inInterval(q.from, q.to, _levels[i].hdr->_minTime) &&
          !inInterval(q.from, q.to, _levels[i].hdr->_maxTime) &&
          !inInterval(_levels[i].hdr->_minTime, _levels[i].hdr->_maxTime, q.from) &&
          !inInterval(_levels[i].hdr->_minTime, _levels[i].hdr->_maxTime, q.to)) {
        continue;
      }
      Meas empty;
      empty.time = q.from;
      auto begin = _levels[i].begin;
      auto end = _levels[i].begin + _levels[i].hdr->pos;
      auto start =
          std::lower_bound(begin, end, empty, [](const Meas &left, const Meas &right) {
            return left.time < right.time;
          });
      for (auto it = start; it != end; ++it) {
        auto m = *it;
        if (m.time > q.to) {
          break;
        }
        if (m.inQuery(q.ids, q.flag, q.source, q.from, q.to)) {
          clbk->call(m);
        }
      }
    }

    for (size_t j = 0; j < _header->_memvalues_pos; ++j) {
      auto m = _memvalues[j];
      if (m.inQuery(q.ids, q.flag, q.source, q.from, q.to)) {
        clbk->call(m);
      }
    }
  }

  Meas::MeasArray readAll() const {
      TIMECODE_METRICS(ctmd, "foreach", "Capacitor::readall");
      boost::shared_lock<boost::shared_mutex> lock(_mutex);

      Meas::MeasArray result;
      result.resize(this->_header->_writed);
      size_t pos=0;

      for (size_t j = 0; j < _header->_memvalues_pos; ++j) {
        auto m = _memvalues[j];
        result[pos++]=m;
      }

      for (size_t i = 0; i < this->_levels.size(); ++i) {
        if (_levels[i].empty()) {
          continue;
        }
        auto begin = _levels[i].begin;
        auto end = _levels[i].begin + _levels[i].hdr->pos;
        for (auto it = begin; it != end; ++it) {
          auto m = *it;
          result[pos++]=m;
        }
      }
      std::sort(result.begin(),result.end(), meas_time_compare_less());
      return result;
  }

  Meas::MeasList readInterval(const QueryInterval &q) {
    TIMECODE_METRICS(ctmd, "readInterval", "Capacitor::readInterval");
    Meas::MeasList result;

    std::unordered_map<dariadb::Id, std::set<Meas, meas_time_compare_less>> sub_result;
    sub_result.reserve(q.ids.size());

    std::unique_ptr<MList_ReaderClb> clbk{new MList_ReaderClb};
    this->foreach (q, clbk.get());
    for (auto &v : clbk->mlist) {
      sub_result[v.id].insert(v);
    }

    for (auto id : q.ids) {
      auto sublist = sub_result.find(id);
      if (sublist == sub_result.end()) {
        continue;
      }
      for (auto v : sublist->second) {
        result.push_back(v);
      }
    }
    return result;
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

  Meas::Id2Meas readInTimePoint(const QueryTimePoint &q) {
    TIMECODE_METRICS(ctmd, "readInTimePoint", "Capacitor::readInTimePoint");
    boost::shared_lock<boost::shared_mutex> lock(_mutex);
    dariadb::Meas::Id2Meas sub_res = timePointValues(q);

    return sub_res;
  }

  Meas::Id2Meas currentValue(const IdArray &ids, const Flag &flag) {
    boost::shared_lock<boost::shared_mutex> lock(_mutex);
    return readInTimePoint(QueryTimePoint(ids, flag, this->maxTime()));
  }

  dariadb::Time minTime() const {
    boost::shared_lock<boost::shared_mutex> lock(_mutex);
    return _header->minTime;
  }

  dariadb::Time maxTime() const {
    boost::shared_lock<boost::shared_mutex> lock(_mutex);
    return _header->maxTime;
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult, dariadb::Time *maxResult) {
    TIMECODE_METRICS(ctmd, "minMaxTime", "Capacitor::minMaxTime");
    boost::shared_lock<boost::shared_mutex> lock(_mutex);

    if (!_header->check_id(id)) {
      return false;
    }

    *minResult = dariadb::MAX_TIME;
    *maxResult = dariadb::MIN_TIME;
    bool result = false;
    for (size_t j = 0; j < _header->_memvalues_pos; ++j) {
      auto m = _memvalues[j];
      if (m.id == id) {
        *minResult = std::min(*minResult, m.time);
        *maxResult = std::max(*maxResult, m.time);
        result = true;
      }
    }

    for (size_t i = 0; i < this->_levels.size(); ++i) {
      if (_levels[i].empty()) {
        continue;
      }

      if (!_levels[i].check_id(id)) {
        continue;
      }
      for (size_t j = 0; j < _levels[i].hdr->pos; ++j) {
        auto m = _levels[i].at(j);
        if (m.id == id) {
          *minResult = std::min(*minResult, m.time);
          *maxResult = std::max(*maxResult, m.time);
          result = true;
        }
      }
    }
    return result;
  }

  void flush() {
    boost::upgrade_lock<boost::shared_mutex> lock(_mutex);
    mmap->flush();
  }

  size_t files_count() const { return Manifest::instance()->cola_list().size(); }

  size_t levels_count() const { return _levels.size(); }

  size_t size() const { return _header->_writed; }

  Meas::Id2Meas timePointValues(const QueryTimePoint &q) {
    IdSet readed_ids;
    Meas::Id2Meas sub_res;
    bool id_exists = _header->check_id(q.ids);
    bool flag_exists = false;
    if (q.flag == Flag(0) || _header->check_flag(q.flag)) {
      flag_exists = true;
    }
    if (!id_exists || !flag_exists) {
      /// no values for query. return no_data for each id.
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

    if (inInterval(_header->minTime, _header->maxTime, q.time_point)) {
      for (size_t j = 0; j < _header->_memvalues_pos; ++j) {
        auto m = _memvalues[j];
        if (m.inQuery(q.ids, q.flag, q.source) && (m.time <= q.time_point)) {
          insert_if_older(sub_res, m);
          readed_ids.insert(m.id);
        }
      }
    }

    for (size_t i = 0; i < this->_levels.size(); ++i) {
      if (_levels[i].empty()) {
        continue;
      }
      if (!inInterval(_levels[i].hdr->_minTime, _levels[i].hdr->_maxTime, q.time_point)) {
        continue;
      }

      id_exists = _levels[i].check_id(q.ids);
      if (!id_exists) {
        continue;
      }

      Meas empty;
      empty.time = q.time_point;
      auto begin = _levels[i].begin;
      auto end = _levels[i].begin + _levels[i].hdr->pos;
      auto start =
          std::lower_bound(begin, end, empty, [](const Meas &left, const Meas &right) {
            return left.time < right.time;
          });

      for (auto it = start; it != end; ++it) {
        auto m = *it;
        if (m.time > q.time_point) {
          break;
        }
        if (m.inQuery(q.ids, q.flag, q.source) && (m.time <= q.time_point)) {
          insert_if_older(sub_res, m);
          readed_ids.insert(m.id);
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

  Header *header() { return _header; }

protected:
  Capacitor::Params _params;

  dariadb::utils::fs::MappedFile::MapperFile_ptr mmap;
  Header *_header;
  uint8_t *_raw_data;
  std::vector<level> _levels;
  size_t _size;
  Meas *_memvalues;
  size_t _memvalues_size;

  mutable boost::shared_mutex _mutex;
  bool _is_readonly;
};

Capacitor::~Capacitor() {}

Capacitor::Capacitor(const Capacitor::Params &params, const std::string &fname,
                     bool readonly)
    : _Impl(new Capacitor::Private(params, fname, readonly)) {}

Capacitor::Header Capacitor::readHeader(std::string file_name) {
  std::ifstream istream;
  istream.open(file_name, std::fstream::in | std::fstream::binary);
  if (!istream.is_open()) {
    std::stringstream ss;
    ss << "can't open file. filename=" << file_name;
    throw MAKE_EXCEPTION(ss.str());
  }
  Capacitor::Header result;
  memset(&result, 0, sizeof(Capacitor::Header));
  istream.read((char *)&result, sizeof(Capacitor::Header));
  istream.close();
  return result;
}

Capacitor::Header *Capacitor::header() {
  return _Impl->header();
}

Time Capacitor::minTime() {
  return _Impl->minTime();
}

Time Capacitor::maxTime() {
  return _Impl->maxTime();
}

bool Capacitor::minMaxTime(Id id, Time *minResult, Time *maxResult) {
  return _Impl->minMaxTime(id, minResult, maxResult);
}
void Capacitor::flush() {
  _Impl->flush();
}

append_result Capacitor::append(const Meas &value) {
  return _Impl->append(value);
}

append_result Capacitor::append(const Meas::MeasArray::const_iterator &begin,
                                const Meas::MeasArray::const_iterator &end) {
  return _Impl->append(begin, end);
}

void Capacitor::foreach (const QueryInterval &q, IReaderClb * clbk) {
  return _Impl->foreach (q, clbk);
}

Meas::MeasList Capacitor::readInterval(const QueryInterval &q) {
  return _Impl->readInterval(q);
}

Meas::MeasArray Capacitor::readAll()const{
    return _Impl->readAll();
}

Meas::Id2Meas Capacitor::readInTimePoint(const QueryTimePoint &q) {
  return _Impl->readInTimePoint(q);
}

Meas::Id2Meas Capacitor::currentValue(const IdArray &ids, const Flag &flag) {
  return _Impl->currentValue(ids, flag);
}

size_t Capacitor::files_count() const {
  return _Impl->files_count();
}

size_t Capacitor::levels_count() const {
  return _Impl->levels_count();
}

size_t Capacitor::size() const {
  return _Impl->size();
}

void Capacitor::drop_to_stor(IMeasWriter *stor) {
  _Impl->drop_to_stor(stor);
}

void Capacitor::fsck() {
  _Impl->fsck();
}

void Capacitor::close() {
  _Impl->close();
}
