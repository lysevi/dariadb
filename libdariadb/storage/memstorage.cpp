#include <libdariadb/storage/memstorage.h>
#include <libdariadb/flags.h>
#include <memory>
#include <tuple>
#include <unordered_map>
#include <cstring>
#include <cassert>

using namespace dariadb;
using namespace dariadb::storage;

MemChunkAllocator::MemChunkAllocator(size_t maxSize, size_t bufferSize) {
  _maxSize = maxSize;
  _bufferSize = bufferSize;
  allocated = size_t(0);
  size_t one_sz = sizeof(ChunkHeader) + bufferSize;
  _total_count = (int)(float(_maxSize) / one_sz);

  _headers.resize(_total_count);
  _buffers = new uint8_t[_total_count * _bufferSize];
  _free_list.resize(_total_count);
  std::fill(_free_list.begin(), _free_list.end(), uint8_t(0));
}

MemChunkAllocator::~MemChunkAllocator() {
  delete[] _buffers;
}

MemChunkAllocator::allocated_data MemChunkAllocator::allocate() {
  _locker.lock();
  for (size_t i = 0; i < _total_count; ++i) {
    if (_free_list[i] == uint8_t(0)) {
      _free_list[i] = 1;
      allocated++;
      _locker.unlock();
	  memset(&_headers[i], 0, sizeof(ChunkHeader));
	  memset(&_buffers[i * _bufferSize], 0, sizeof(_bufferSize));
      return allocated_data(&_headers[i], &_buffers[i * _bufferSize], i);
    }
  }
  _locker.unlock();
  return EMPTY;
}

void MemChunkAllocator::free(const MemChunkAllocator::allocated_data &d) {
  auto header = std::get<0>(d);
  auto buffer = std::get<1>(d);
  auto position = std::get<2>(d);
  memset(header, 0, sizeof(ChunkHeader));
  memset(buffer, 0, _bufferSize);
  _locker.lock();
  allocated--;
  _free_list[position] = 0;
  _locker.unlock();
}

/**
Map:
  Meas.id -> TimeTrack{ MemChunkList[MemChunk{data}]}
*/

struct MemChunk : public ZippedChunk {
  ChunkHeader *index_ptr;
  uint8_t *buffer_ptr;
  MemChunkAllocator::allocated_data _a_data;
  MemChunk(ChunkHeader *index, uint8_t *buffer, size_t size, Meas first_m)
      : ZippedChunk(index, buffer, size, first_m) {
    index_ptr = index;
    buffer_ptr = buffer;
  }
  MemChunk(ChunkHeader *index, uint8_t *buffer) : ZippedChunk(index, buffer) {
    index_ptr = index;
    buffer_ptr = buffer;
  }
  ~MemChunk() {}
};

using MemChunk_Ptr = std::shared_ptr<MemChunk>;
using MemChunkList = std::list<MemChunk_Ptr>;

class TimeTrack : public IMeasStorage {
public:
  TimeTrack(const Time step, Id meas_id, MemChunkAllocator*allocator) {
	  _allocator = allocator;
    _meas_id = meas_id;
    _step = step;
	_minTime = MAX_TIME;
	_maxTime = MIN_TIME;
  }
  ~TimeTrack() {}

  MemChunkAllocator*_allocator;
  Id _meas_id;
  Time _minTime;
  Time _maxTime;
  Time _step;
  MemChunkList _chunks;
  MemChunk_Ptr _cur_chunk;
  utils::Locker _locker;

  void updateMinMax(const Meas&value) {
	  _minTime = std::min(_minTime, value.time);
	  _maxTime = std::max(_maxTime, value.time);
  }
  virtual append_result append(const Meas &value) override {
    std::lock_guard<utils::Locker> lg(_locker);
    if (_chunks.empty() || _chunks.back()->is_full()) {
		if (!create_new_chunk(value)) {
			return append_result(0,1);
		}
		else {
			updateMinMax(value);
			return append_result(1, 0);
		}
    }
	if (!_cur_chunk->append(value)) {
		if (!create_new_chunk(value)) {
			return append_result(0, 1);
		}
		else {
			updateMinMax(value);
			return append_result(1, 0);
		}
	}
	updateMinMax(value);
    return append_result(1, 0);
  }

  virtual void flush() {}
  
  virtual Time minTime() override {
	  return _minTime;
  }

  virtual Time maxTime() override { 
	  return _maxTime;
  }

  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) override {
	  if (id != this->_meas_id) {
		  return false;
	  }    
	  std::lock_guard<utils::Locker> lg(_locker);
	  *minResult = MAX_TIME;
	  *maxResult = MIN_TIME;
	  for (auto c : _chunks) {
		  *minResult = std::min(c->header->minTime, *minResult);
		  *maxResult = std::max(c->header->maxTime, *maxResult);
	  }
	  return true;
  }

  //TODO use b+tree for fast search;
  virtual void foreach (const QueryInterval &q, IReaderClb * clbk) override {
    std::lock_guard<utils::Locker> lg(_locker);
    for (auto c : _chunks) {
      if (utils::inInterval(c->header->minTime, c->header->maxTime, q.from)
		  ||utils::inInterval(c->header->minTime, c->header->maxTime, q.to)
		  || utils::inInterval(q.from, q.to, c->header->minTime)
		  || utils::inInterval(q.from, q.to, c->header->maxTime)) {
        auto rdr = c->get_reader();

        while (!rdr->is_end()) {
          auto v = rdr->readNext();
		  if (utils::inInterval(q.from, q.to, v.time)) {
			  clbk->call(v);
		  }
        }
      }
    }
  }

  //TODO use b+tree for fast search;
  virtual Id2Meas readTimePoint(const QueryTimePoint &q) override { 
	  std::lock_guard<utils::Locker> lg(_locker);
	  Id2Meas result;
	  result[this->_meas_id].flag = Flags::_NO_DATA;
	  for (auto c : _chunks) {
		  if (c->header->minTime >= q.time_point && c->header->maxTime <= q.time_point) {
			  auto rdr = c->get_reader();

			  while (!rdr->is_end()) {
				  auto v = rdr->readNext();
				  if (v.time > result[this->_meas_id].time && v.time<=q.time_point) {
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

  virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override {
	  assert(ids.size() = size_t(1));
	  std::lock_guard<utils::Locker> lg(_locker);
	  Id2Meas result;
	  result[_meas_id] = _chunks.back()->header->last;
	  return result;
  }


  bool create_new_chunk(const Meas&value) {
	  auto new_chunk_data = _allocator->allocate();
	  if (std::get<0>(new_chunk_data) == nullptr) {
		  return false;
	  }
	  auto mc = MemChunk_Ptr{ new MemChunk{ std::get<0>(new_chunk_data),
		  std::get<1>(new_chunk_data),
		  _allocator->_bufferSize, value } };
	  this->_chunks.push_back(mc);
	  _cur_chunk = mc;
	  return true;
  }
};

using TimeTrack_ptr = std::shared_ptr<TimeTrack>;
using Id2Track = std::unordered_map<Id, TimeTrack_ptr>;

struct MemStorage::Private : public IMeasStorage {
	Private(const MemStorage::Params &p) : _chunk_allocator(p.max_size, p.chunk_size) {}

  append_result append(const Meas &value) override { 
	  auto track = _id2track.find(value.id);
	  if (track == _id2track.end()) {
		  std::lock_guard<utils::Locker> lg(_all_tracks_locker);
		  track = _id2track.find(value.id);
		  if (track == _id2track.end()) {
			  auto new_track = std::make_shared<TimeTrack>(Time(0), value.id, &_chunk_allocator);
			  _id2track.insert(std::make_pair(value.id, new_track));
			  new_track->append(value);
			  return append_result(1,0);
		  }
	  }
	  return track->second->append(value);
  }
  Time minTime() override { 
	  Time result = MAX_TIME;
	  for (auto t : _id2track) {
		  result=std::min(result, t.second->minTime());
	  }
	  return result;
  }
  virtual Time maxTime() override { 
	  Time result = MIN_TIME;
	  for (auto t : _id2track) {
		  result = std::max(result, t.second->maxTime());
	  }
	  return result;
  }
  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) override {
	  auto tracker = _id2track.find(id);
	  if (tracker != _id2track.end()) {
		  return tracker->second->minMaxTime(id, minResult, maxResult);
	  }
	  return false;
  }
  
  void foreach (const QueryInterval &q, IReaderClb * clbk) override {
	  QueryInterval local_q({}, q.flag, q.from,q.to);
	  local_q.ids.resize(1);
	  for (auto id : q.ids) {
		  auto tracker = _id2track.find(id);
		  if (tracker != _id2track.end()) {
			  local_q.ids[0] = id;
			  tracker->second->foreach(local_q,clbk);
		  }
	  }
  }

  virtual Id2Meas readTimePoint(const QueryTimePoint &q) override { 
	  QueryTimePoint local_q({}, q.flag, q.time_point);
	  local_q.ids.resize(1);
	  Id2Meas result;
	  for (auto id : q.ids) {
		  auto tracker = _id2track.find(id);
		  if (tracker != _id2track.end()) {
			  local_q.ids[0] = id;
			  auto sub_res = tracker->second->readTimePoint(local_q);
			  result[id] = sub_res[id];
		  }
		  else {
			  result[id].flag = Flags::_NO_DATA;
		  }
	  }
	  return result;
  }
  virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override {
	  IdArray local_ids;
	  local_ids.reserve(1);
	  Id2Meas result;
	  for (auto id : ids) {
		  auto tracker = _id2track.find(id);
		  if (tracker != _id2track.end()) {
			  local_ids[0] = id;
			  auto sub_res = tracker->second->currentValue(local_ids, flag);
			  result[id] = sub_res[id];
		  }
		  else {
			  result[id].flag = Flags::_NO_DATA;
		  }
	  }
	  return result;
  }


  void flush() override {}

  Id2Track _id2track;
  MemChunkAllocator _chunk_allocator;
  utils::Locker _all_tracks_locker;
};

MemStorage::MemStorage(const MemStorage::Params &p) : _impl(new MemStorage::Private(p)) {}

MemStorage::~MemStorage() {
  _impl = nullptr;
}

Time dariadb::storage::MemStorage::minTime() {
  return _impl->minTime();
}

Time dariadb::storage::MemStorage::maxTime() {
  return _impl->maxTime();
}

bool dariadb::storage::MemStorage::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                                              dariadb::Time *maxResult) {
  return _impl->minMaxTime(id, minResult, maxResult);
}

void dariadb::storage::MemStorage::foreach (const QueryInterval &q, IReaderClb * clbk) {
  _impl->foreach (q, clbk);
}

Id2Meas dariadb::storage::MemStorage::readTimePoint(const QueryTimePoint &q) {
  return _impl->readTimePoint(q);
}

Id2Meas dariadb::storage::MemStorage::currentValue(const IdArray &ids, const Flag &flag) {
  return _impl->currentValue(ids, flag);
}

append_result dariadb::storage::MemStorage::append(const Meas &value) {
  return _impl->append(value);
}

void dariadb::storage::MemStorage::flush() {
  _impl->flush();
}
