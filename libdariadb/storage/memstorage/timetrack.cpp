#ifdef MSVC
#define _SCL_SECURE_NO_WARNINGS // stx::btree
#endif
#include <libdariadb/flags.h>
#include <libdariadb/storage/cursors.h>
#include <libdariadb/storage/memstorage/timetrack.h>

using namespace dariadb;
using namespace dariadb::storage;

struct MemTrackReader : dariadb::ICursor {
  MemTrackReader(const Cursor_Ptr &r, const TimeTrack_ptr &track) {
    _r = r;
    _track = track;
    ENSURE(track != nullptr);
    ENSURE(r != nullptr);
    _track->is_locked_to_drop = true;
  }

  ~MemTrackReader() { _track->is_locked_to_drop = false; }
  Meas readNext() override { return _r->readNext(); }

  Meas top() override { return _r->top(); }
  bool is_end() const override { return _r->is_end(); }

  Time minTime() override { return _track->minTime(); }

  Time maxTime() override { return _track->minTime(); }

  Cursor_Ptr _r;
  TimeTrack_ptr _track;
};

TimeTrack::TimeTrack(MemoryChunkContainer *mcc, const Time step, Id meas_id,
                     MemChunkAllocator *allocator) {
  _allocator = allocator;
  _meas_id = meas_id;
  _step = step;
  _min_max.min.time = MAX_TIME;
  _min_max.max.time = MIN_TIME;
  _max_sync_time = MIN_TIME;
  _mcc = mcc;
  is_locked_to_drop = false;
}

TimeTrack::~TimeTrack() {}

void TimeTrack::updateMinMax(const Meas &value) {
  _min_max.updateMax(value);
  _min_max.updateMin(value);
}

Status TimeTrack::append(const Meas &value) {
  std::lock_guard<utils::async::Locker> lg(_locker);
  if (_cur_chunk == nullptr || _cur_chunk->isFull()) {
    if (!create_new_chunk(value)) {
      return Status(0, 1);
    } else {
      updateMinMax(value);
      return Status(1, 0);
    }
  }
  if (_cur_chunk->header->stat.maxTime < value.time) {
    if (!_cur_chunk->append(value)) {
      if (!create_new_chunk(value)) {
        return Status(0, 1);
      } else {
        updateMinMax(value);
      }
    }
  } else { // insertion to past
    append_to_past(value);
  }
  updateMinMax(value);
  return Status(1, 0);
}

void TimeTrack::append_to_past(const Meas &value) {
  auto is_cur_chunk = false;
  MemChunk_Ptr target_to_replace = nullptr;

  // find target chunk.
  if (_index.empty() || _cur_chunk->header->stat.minTime < value.time) {
    target_to_replace = _cur_chunk;
    is_cur_chunk = true;
    ENSURE(target_to_replace.use_count() <= long(3));
  } else {
    target_to_replace = get_target_to_replace_from_index(value.time);
  }

  ENSURE(target_to_replace != nullptr);
  MemChunk_Ptr new_chunk = nullptr;

  MeasSet mset;
  if (is_cur_chunk) {
    _cur_chunk = nullptr;
  } else {
    _index.erase(target_to_replace->header->stat.maxTime);
  }

  if (target_to_replace->_is_from_pool) {
    _mcc->freeChunk(target_to_replace);
  }
  /// unpack and sort.
  auto rdr = target_to_replace->getReader();
  while (!rdr->is_end()) {
    auto v = rdr->readNext();
    if (v.time == value.time) {
      continue;
    }
    mset.insert(v);
  }
  rdr = nullptr;

  mset.insert(value);

  /// need to leak detection.
  ENSURE(target_to_replace.use_count() == long(1));
  auto old_chunk_size = target_to_replace->header->size;
  target_to_replace = nullptr;

  MeasArray mar{mset.begin(), mset.end()};
  ENSURE(mar.front().time <= mar.back().time);

  auto buffer_size = old_chunk_size + sizeof(Meas) * 2;
  uint8_t *new_buffer = new uint8_t[buffer_size];
  std::fill_n(new_buffer, buffer_size, uint8_t(0));
  ChunkHeader *hdr = new ChunkHeader;

  new_chunk = MemChunk_Ptr{
      new MemChunk(false, hdr, new_buffer, buffer_size, mar.front())};
  new_chunk->_track = this;
  for (size_t i = 1; i < mar.size(); ++i) {
    auto v = mar[i];
    auto status = new_chunk->append(v);
    if (!status) {
      THROW_EXCEPTION("logic error.");
    }
  }

  if (is_cur_chunk) {
    _cur_chunk = new_chunk;
  } else {
    _index.insert(std::make_pair(new_chunk->header->stat.maxTime, new_chunk));
  }
}

MemChunk_Ptr TimeTrack::get_target_to_replace_from_index(const Time t) {
  using utils::inInterval;

  MemChunk_Ptr target_to_replace = nullptr;

  std::vector<MemChunk_Ptr> potential_targets;
  potential_targets.reserve(_index.size());
  if (_index.size() == 1) {
    potential_targets.push_back(_index.begin()->second);
  } else {
    auto end = _index.upper_bound(t);
    auto begin = _index.lower_bound(t);
    if (end != _index.end()) {
      ++end;
    }
    for (auto it = begin; it != end; ++it) {
      potential_targets.push_back(it->second);
    }
  }

  if (potential_targets.size() == size_t(1)) {
    target_to_replace = potential_targets.front();
  } else {
    for (size_t i = 0;; ++i) {
      auto next = potential_targets[i + 1];
      auto c = potential_targets[i];

      if (next->header->stat.minTime > t ||
          inInterval(c->header->stat.minTime, c->header->stat.maxTime, t)) {
        target_to_replace = c;
        break;
      }
      if (i == potential_targets.size() - 1) {
        target_to_replace = potential_targets.back();
      }
    }
  }

  ENSURE(target_to_replace != nullptr);
  return target_to_replace;
}

void TimeTrack::flush() {}

Time TimeTrack::TimeTrack::minTime() { return _min_max.min.time; }

Time TimeTrack::maxTime() { return _min_max.max.time; }

bool TimeTrack::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                           dariadb::Time *maxResult) {
  if (id != this->_meas_id) {
    return false;
  }
  std::lock_guard<utils::async::Locker> lg(_locker);
  *minResult = MAX_TIME;
  *maxResult = MIN_TIME;
  for (auto kv : _index) {
    auto c = kv.second;
    *minResult = std::min(c->header->stat.minTime, *minResult);
    *maxResult = std::max(c->header->stat.maxTime, *maxResult);
  }
  if (_cur_chunk != nullptr) {
    *minResult = std::min(_cur_chunk->header->stat.minTime, *minResult);
    *maxResult = std::max(_cur_chunk->header->stat.maxTime, *maxResult);
  }
  return true;
}

bool chunkInQuery(Time from, Time to, const Chunk_Ptr &c) {
  if (utils::inInterval(c->header->stat.minTime, c->header->stat.maxTime,
                        from) ||
      utils::inInterval(c->header->stat.minTime, c->header->stat.maxTime, to) ||
      utils::inInterval(from, to, c->header->stat.minTime) ||
      utils::inInterval(from, to, c->header->stat.maxTime)) {
    return true;
  } else {
    return false;
  }
}

bool chunkInQuery(const QueryInterval &q, const Chunk_Ptr &c) {
  return chunkInQuery(q.from, q.to, c);
}

Id2Cursor TimeTrack::intervalReader(const QueryInterval &q) {
  std::lock_guard<utils::async::Locker> lg(_locker);

  CursorsList readers;
  auto end = _index.upper_bound(q.to);
  auto begin = _index.lower_bound(q.from);
  if (begin != _index.begin()) {
    --begin;
  }
  for (auto it = begin; it != end; ++it) {
    if (it == _index.end()) {
      break;
    }
    auto c = it->second;
    if (chunkInQuery(q, c)) {
      auto rdr = c->getReader();
      readers.push_back(rdr);
    }
  }
  if (_cur_chunk != nullptr && chunkInQuery(q, _cur_chunk)) {
    auto rdr = _cur_chunk->getReader();
    readers.push_back(rdr);
  }
  if (readers.empty()) {
    return Id2Cursor();
  }
  Cursor_Ptr result = CursorWrapperFactory::colapseCursors(readers);
  Id2Cursor i2r;
  i2r[this->_meas_id] = result;
  return i2r;
}

Statistic TimeTrack::stat(const Id id, Time from, Time to) {
  std::lock_guard<utils::async::Locker> lg(_locker);
  ENSURE(id == this->_meas_id);
  Statistic result;
  auto end = _index.upper_bound(to);
  auto begin = _index.lower_bound(from);
  if (begin != _index.begin()) {
    --begin;
  }

  for (auto it = begin; it != end; ++it) {
    if (it == _index.end()) {
      break;
    }
    auto c = it->second;
    if (chunkInQuery(from, to, c)) {
      auto st = c->stat(from, to);
      result.update(st);
    }
  }

  if (_cur_chunk != nullptr && chunkInQuery(from, to, _cur_chunk)) {
    auto st = _cur_chunk->stat(from, to);
    result.update(st);
  }
  return result;
}

void TimeTrack::foreach (const QueryInterval &q, IReadCallback * clbk) {
  auto rdrs = intervalReader(q);
  if (rdrs.empty()) {
    return;
  }
  for (auto kv : rdrs) {
    kv.second->apply(clbk, q);
  }
}

Id2Meas TimeTrack::readTimePoint(const QueryTimePoint &q) {
  std::lock_guard<utils::async::Locker> lg(_locker);
  Id2Meas result;
  result[this->_meas_id].flag = FLAGS::_NO_DATA;

  auto end = _index.upper_bound(q.time_point);
  auto begin = _index.lower_bound(q.time_point);
  if (begin != _index.begin()) {
    --begin;
  }

  if (end != _index.end()) {
    ++end;
  }

  for (auto it = begin; it != end; ++it) {
    if (it == _index.end()) {
      break;
    }
    auto c = it->second;

    if (c->header->stat.minTime <= q.time_point &&
        c->header->stat.maxTime >= q.time_point) {
      auto rdr = c->getReader();
      auto m = rdr->read_time_point(q);
      if (m.time > result[this->_meas_id].time) {
        result[this->_meas_id] = m;
      }
    }
  }

  if (_cur_chunk != nullptr &&
      _cur_chunk->header->stat.minTime >= q.time_point &&
      _cur_chunk->header->stat.maxTime <= q.time_point) {
    auto rdr = _cur_chunk->getReader();
    while (!rdr->is_end()) {
      auto v = rdr->readNext();
      if (v.time > result[this->_meas_id].time && v.time <= q.time_point) {
        result[this->_meas_id] = v;
      }
    }
  }
  if (result[this->_meas_id].flag == FLAGS::_NO_DATA) {
    result[this->_meas_id].time = q.time_point;
  }
  return result;
}

Id2Meas TimeTrack::currentValue(const IdArray &ids, const Flag &flag) {
  ENSURE(ids.size() == size_t(1));
  ENSURE(ids[0] == this->_meas_id);
  std::lock_guard<utils::async::Locker> lg(_locker);
  Id2Meas result;
  if (_cur_chunk != nullptr) {
    auto last = _cur_chunk->header->last();
    if (last.inFlag(flag)) {
      result[_meas_id] = last;
      return result;
    }
  }
  result[_meas_id].flag = FLAGS::_NO_DATA;
  return result;
}

void TimeTrack::rm_chunk(MemChunk *c) {
  std::lock_guard<utils::async::Locker> lg(_locker);
  _index.erase(c->header->stat.maxTime);

  if (_cur_chunk.get() == c) {
    _cur_chunk = nullptr;
  }
}

void TimeTrack::rereadMinMax() {
  std::lock_guard<utils::async::Locker> lg(_locker);
  _min_max.max.time = MIN_TIME;
  _min_max.min.time = MIN_TIME;

  if (_index.size() != 0) {
    auto c = _index.begin()->second;
    _min_max.min = c->header->first();
  } else {
    if (this->_cur_chunk != nullptr) {
      _min_max.min = _cur_chunk->header->first();
    }
  }

  if (this->_cur_chunk != nullptr) {
    _min_max.max = _cur_chunk->header->last();
  }
}

bool TimeTrack::create_new_chunk(const Meas &value) {
  if (_cur_chunk != nullptr) {
    this->_index.insert(
        std::make_pair(_cur_chunk->header->stat.maxTime, _cur_chunk));
    _cur_chunk = nullptr;
  }
  auto new_chunk_data = _allocator->allocate();
  if (new_chunk_data.header == nullptr) {
    return false;
  }
  auto mc = MemChunk_Ptr{new MemChunk{true, new_chunk_data.header,
                                      new_chunk_data.buffer,
                                      _allocator->_chunkSize, value}};
  mc->_track = this;
  mc->_a_data = new_chunk_data;
  this->_mcc->addChunk(mc);
  _cur_chunk = mc;
  return true;
}
