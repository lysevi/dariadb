#ifdef MSVC
#define _SCL_SECURE_NO_WARNINGS // stx::btree
#endif
#include <libdariadb/flags.h>
#include <libdariadb/storage/memstorage/timetrack.h>

using namespace dariadb;
using namespace dariadb::storage;

TimeTrack::TimeTrack(MemoryChunkContainer *mcc, const Time step, Id meas_id,
                     MemChunkAllocator *allocator) {
  _allocator = allocator;
  _meas_id = meas_id;
  _step = step;
  _min_max.min.time = MAX_TIME;
  _min_max.max.time = MIN_TIME;
  _max_sync_time = MIN_TIME;
  _mcc = mcc;
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
        return Status(1, 0);
      }
    }
  } else {
    logger_fatal("engine: memstorage - id:", this->_meas_id, ", can't write to past.");
    return Status(1, 1);
  }
  updateMinMax(value);
  return Status(1, 0);
}

void TimeTrack::flush() {}

Time TimeTrack::TimeTrack::minTime() {
  return _min_max.min.time;
}

Time TimeTrack::maxTime() {
  return _min_max.max.time;
}

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

void TimeTrack::foreach (const QueryInterval &q, IReaderClb * clbk) {
  std::lock_guard<utils::async::Locker> lg(_locker);

  auto end = _index.upper_bound(q.to);
  auto begin = _index.lower_bound(q.from);
  if (begin != _index.begin()) {
    --begin;
  }
  for (auto it = begin; it != end; ++it) {
    if (clbk->is_canceled()) {
      break;
    }
    if (it == _index.end()) {
      break;
    }
    auto c = it->second;
    foreach_interval_call(c, q, clbk);
  }
  if (_cur_chunk != nullptr) {
    foreach_interval_call(_cur_chunk, q, clbk);
  }
}

void TimeTrack::foreach_interval_call(const MemChunk_Ptr &c, const QueryInterval &q,
                                      IReaderClb *clbk) {
  if (utils::inInterval(c->header->stat.minTime, c->header->stat.maxTime, q.from) ||
      utils::inInterval(c->header->stat.minTime, c->header->stat.maxTime, q.to) ||
      utils::inInterval(q.from, q.to, c->header->stat.minTime) ||
      utils::inInterval(q.from, q.to, c->header->stat.maxTime)) {

    auto rdr = c->getReader();
    while (!rdr->is_end()) {
      if (clbk->is_canceled()) {
        break;
      }
      auto v = rdr->readNext();
      if (utils::inInterval(q.from, q.to, v.time)) {
        clbk->call(v);
      }
    }
  }
}

Id2Meas TimeTrack::readTimePoint(const QueryTimePoint &q) {
  std::lock_guard<utils::async::Locker> lg(_locker);
  Id2Meas result;
  result[this->_meas_id].flag = Flags::_NO_DATA;

  auto end = _index.upper_bound(q.time_point);
  auto begin = _index.lower_bound(q.time_point);
  if (begin != _index.begin()) {
    --begin;
  }

  for (auto it = begin; it != end; ++it) {
    if (it == _index.end()) {
      break;
    }
    auto c = it->second;
    if (c->header->stat.minTime >= q.time_point && c->header->stat.maxTime <= q.time_point) {
      auto rdr = c->getReader();
      // auto skip = c->in_disk_count;
      while (!rdr->is_end()) {
        auto v = rdr->readNext();
        /*if (skip != 0) {
        --skip;
        }
        else*/
        {
          if (v.time > result[this->_meas_id].time && v.time <= q.time_point) {
            result[this->_meas_id] = v;
          }
        }
      }
    }
  }

  if (_cur_chunk != nullptr && _cur_chunk->header->stat.minTime >= q.time_point &&
      _cur_chunk->header->stat.maxTime <= q.time_point) {
    auto rdr = _cur_chunk->getReader();
    // auto skip = _cur_chunk->in_disk_count;
    while (!rdr->is_end()) {
      auto v = rdr->readNext();
      /*if (skip != 0)
      {
      --skip;
      }
      else*/
      {
        if (v.time > result[this->_meas_id].time && v.time <= q.time_point) {
          result[this->_meas_id] = v;
        }
      }
    }
  }
  if (result[this->_meas_id].flag == Flags::_NO_DATA) {
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
  result[_meas_id].flag = Flags::_NO_DATA;
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
    this->_index.insert(std::make_pair(_cur_chunk->header->stat.maxTime, _cur_chunk));
    _cur_chunk = nullptr;
  }
  auto new_chunk_data = _allocator->allocate();
  if (new_chunk_data.header == nullptr) {
    return false;
  }
  auto mc = MemChunk_Ptr{new MemChunk{new_chunk_data.header, new_chunk_data.buffer,
                                      _allocator->_chunkSize, value}};
  mc->_track = this;
  mc->_a_data = new_chunk_data;
  this->_mcc->addChunk(mc);
  _cur_chunk = mc;
  return true;
}
