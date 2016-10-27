#define _SCL_SECURE_NO_WARNINGS //stx::btree
#include <libdariadb/storage/memstorage.h>
#include <libdariadb/flags.h>
#include <stx/btree_map>
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
  _allocated = size_t(0);
  size_t one_chunk_size = sizeof(ChunkHeader) + bufferSize;
  _capacity = (int)(float(_maxSize) / one_chunk_size);
  size_t buffers_size=_capacity * _bufferSize;

  _headers.resize(_capacity);
  _buffers = new uint8_t[buffers_size];

  memset(_buffers,0,buffers_size);
  for(size_t i=0;i<_capacity;++i){
      memset(&_headers[i],0,sizeof(ChunkHeader));
      _free_list.push_back(i);
  }
}

MemChunkAllocator::~MemChunkAllocator() {
  delete[] _buffers;
}

MemChunkAllocator::allocated_data MemChunkAllocator::allocate() {
  _locker.lock();
  if(_free_list.empty()){
      _locker.unlock();
      return EMPTY;
  }
  auto pos=_free_list.front();
  _free_list.pop_front();
  _allocated++;
  _locker.unlock();
  return allocated_data(&_headers[pos], &_buffers[pos * _bufferSize], pos);
}

void MemChunkAllocator::free(const MemChunkAllocator::allocated_data &d) {
  auto header = std::get<0>(d);
  auto buffer = std::get<1>(d);
  auto position = std::get<2>(d);
  memset(header, 0, sizeof(ChunkHeader));
  memset(buffer, 0, _bufferSize);
  _locker.lock();
  _allocated--;
  _free_list.push_back(position);
  _locker.unlock();
}

/**
Map:
  Meas.id -> TimeTrack{ MemChunkList[MemChunk{data}]}
*/
struct TimeTrack;
struct MemChunk : public ZippedChunk {
  ChunkHeader *index_ptr;
  uint8_t *buffer_ptr;
  MemChunkAllocator::allocated_data _a_data;
  MeasList inserted;
  TimeTrack *_track;
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

  void insert(Meas value) {
	  inserted.push_back(value);
  }
};

using MemChunk_Ptr = std::shared_ptr<MemChunk>;
using MemChunkList = std::list<MemChunk_Ptr>;

struct TimeTrack : public IMeasStorage {
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
  stx::btree_map<Time, MemChunk_Ptr> _index;
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
	if (_cur_chunk->header->maxTime < value.time) {
		if (!_cur_chunk->append(value)) {
			if (!create_new_chunk(value)) {
				return append_result(0, 1);
			}
			else {
				updateMinMax(value);
				return append_result(1, 0);
			}
		}
	}
	else {
		auto target_to_insert = _cur_chunk;
		auto begin = _index.lower_bound(value.time);
		auto end = _index.upper_bound(value.time);
		for (auto it = begin; it != end; ++end) {
			if (it == _index.end()) {
				THROW_EXCEPTION("egine: memstorage logic error.");
			}
			if (utils::inInterval(it->second->header->minTime, it->second->header->maxTime, value.time)) {
				target_to_insert = it->second;
				break;
			}
		}
		target_to_insert->insert(value);
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

  virtual void foreach (const QueryInterval &q, IReaderClb * clbk) override {
    std::lock_guard<utils::Locker> lg(_locker);
	auto end=_index.upper_bound(q.to);
	auto begin = _index.lower_bound(q.from);
	if (begin != _index.begin()) {
		--begin;
	}
    for(auto it=begin;it!=end;++it) {
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

  void foreach_interval_call(const MemChunk_Ptr&c, const QueryInterval &q, IReaderClb * clbk) {
	  if (utils::inInterval(c->header->minTime, c->header->maxTime, q.from)
		  || utils::inInterval(c->header->minTime, c->header->maxTime, q.to)
		  || utils::inInterval(q.from, q.to, c->header->minTime)
		  || utils::inInterval(q.from, q.to, c->header->maxTime)) {
		  if (c->inserted.empty()) {
			  auto rdr = c->get_reader();

			  while (!rdr->is_end()) {
				  auto v = rdr->readNext();
				  if (utils::inInterval(q.from, q.to, v.time)) {
					  clbk->call(v);
				  }
			  }
		  }
		  else {
			  auto total_size = c->header->count + c->inserted.size()+1;
			  MeasArray ma;
			  ma.reserve(total_size);
			  auto rdr = c->get_reader();

			  while (!rdr->is_end()) {
				  auto v = rdr->readNext();
				  if (utils::inInterval(q.from, q.to, v.time)) {
					  ma.push_back(v);
				  }
			  }
			  for (auto&v : c->inserted) {
				  ma.push_back(v);
			  }
			  std::sort(ma.begin(), ma.end(), meas_time_compare_less());
			  for (auto&v : ma) {
				  if (utils::inInterval(q.from, q.to, v.time)) {
					  clbk->call(v);
				  }
			  }
		  }
	  }
  }

  Id2Meas readTimePoint(const QueryTimePoint &q) override { 
	  std::lock_guard<utils::Locker> lg(_locker);
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
	  
	  if (_cur_chunk!=nullptr && _cur_chunk->header->minTime >= q.time_point && _cur_chunk->header->maxTime <= q.time_point) {
		  auto rdr = _cur_chunk->get_reader();

		  while (!rdr->is_end()) {
			  auto v = rdr->readNext();
			  if (v.time > result[this->_meas_id].time && v.time <= q.time_point) {
				  result[this->_meas_id] = v;
			  }
		  }
	  }
	  if (result[this->_meas_id].flag == Flags::_NO_DATA) {
		  result[this->_meas_id].time = q.time_point;
	  }
	  return result;
  }

  virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override {
      assert(ids.size() == size_t(1));
      assert(ids[0]==this->_meas_id);
	  std::lock_guard<utils::Locker> lg(_locker);
	  Id2Meas result;
	  auto last = _chunks.back()->header->last;
	  if (last.inFlag(flag)) {
		  result[_meas_id] = _chunks.back()->header->last;
	  }
	  else {
		  result[_meas_id].flag = Flags::_NO_DATA;
	  }
	  return result;
  }

  void rm_chunk(MemChunk *c) {
    _index.erase(c->header->maxTime);
    this->_allocator->free(c->_a_data);
    bool removed = false;
    for (auto it = _chunks.begin(); it != _chunks.end(); ++it) {
      if (it->get() == c) {
        removed = true;
        _chunks.erase(it);
        break;
      }
    }
    if (!removed) {
      THROW_EXCEPTION("engine: memstorage logic error.");
    }
    if (_cur_chunk.get() == c) {
      _cur_chunk = nullptr;
    }
  }

  bool create_new_chunk(const Meas&value) {
	  if (_cur_chunk != nullptr) {
		  this->_index.insert(std::make_pair(_cur_chunk->header->maxTime, _cur_chunk));
	  }
	  auto new_chunk_data = _allocator->allocate();
	  if (std::get<0>(new_chunk_data) == nullptr) {
		  return false;
	  }
	  auto mc = MemChunk_Ptr{ new MemChunk{ std::get<0>(new_chunk_data),
		  std::get<1>(new_chunk_data),
		  _allocator->_bufferSize, value } };
      mc->_track=this;
      mc->_a_data=new_chunk_data;
	  this->_chunks.push_back(mc);
	  _cur_chunk = mc;
	  return true;
  }
};

using TimeTrack_ptr = std::shared_ptr<TimeTrack>;
using Id2Track = std::unordered_map<Id, TimeTrack_ptr>;

struct MemStorage::Private : public IMeasStorage {
	Private(const storage::Settings_ptr &s) : _chunk_allocator(s->memory_limit, s->page_chunk_size) {
		_down_level_storage = nullptr;
		_settings = s;
		if (_settings->id_count != 0) {
			_id2track.reserve(_settings->id_count);
		}
	}

	MemStorage::Description description()const {
		MemStorage::Description result;
        result.allocated = _chunk_allocator._allocated;
        result.allocator_capacity = _chunk_allocator._capacity;
		return result;
	}
  append_result append(const Meas &value) override { 
	  _all_tracks_locker.lock();
	  auto track = _id2track.find(value.id);//TODO refact!
	  if (track == _id2track.end()) {
		  track = _id2track.find(value.id);
		  if (track == _id2track.end()) {
			  auto new_track = std::make_shared<TimeTrack>(Time(0), value.id, &_chunk_allocator);
			  _id2track.insert(std::make_pair(value.id, new_track));
			  _all_tracks_locker.unlock();
			  auto res=new_track->append(value);
			  if (res.writed != 1) {
				  if (_down_level_storage == nullptr) {
					  return res;
				  }
				  _all_tracks_locker.lock();
				  drop_by_limit();
				  _all_tracks_locker.unlock();

				  return append(value);
			  }
			  else {
				  return append_result(1, 0);
			  }
		  }
	  }
      _all_tracks_locker.unlock();
      auto result=track->second->append(value);
	  if (result.ignored == 0) {
		  return result;
	  }
	  else {
          if(_down_level_storage==nullptr){
              return result;
          }
          _all_tracks_locker.lock();
		  drop_by_limit();
          _all_tracks_locker.unlock();
          return append(value);
	  }
  }

  void drop_by_limit() {
    auto current_chunk_count = this->_chunk_allocator._allocated;
    if(current_chunk_count<this->_chunk_allocator._capacity){
        return;
    }
    auto chunks_to_delete =
        (int)(current_chunk_count * _settings->chunks_to_free);
    logger_info("engine: drop ", chunks_to_delete, " chunks of ",
                current_chunk_count);
    std::vector<Chunk*> all_chunks;
    all_chunks.resize(current_chunk_count);
    size_t pos = 0;
    for (auto &kv : _id2track) {
      TimeTrack_ptr t = kv.second;
      for (auto &c : t->_chunks) {
        all_chunks[pos++] = c.get();
      }
    }

    std::sort(all_chunks.begin(), all_chunks.end(),
              [](const Chunk* l, const Chunk* r) {
                return l->header->maxTime < r->header->maxTime;
              });
    assert(all_chunks.front()->header->maxTime<=all_chunks.back()->header->maxTime);

    _down_level_storage->appendChunks(all_chunks,chunks_to_delete);
    for(size_t i=0;i<chunks_to_delete;++i){
		auto c = all_chunks[i];
        auto mc=dynamic_cast<MemChunk*>(c);
        assert(mc!=nullptr);
        TimeTrack *track=mc->_track;
        track->rm_chunk(mc);
        if(track->_chunks.empty()){
            _id2track.erase(track->_meas_id);
        }
    }

  }

  Time minTime() override { 
      std::lock_guard<utils::Locker> lg(_all_tracks_locker);
	  Time result = MAX_TIME;
	  for (auto t : _id2track) {
		  result=std::min(result, t.second->minTime());
	  }
	  return result;
  }
  virtual Time maxTime() override { 
      std::lock_guard<utils::Locker> lg(_all_tracks_locker);
	  Time result = MIN_TIME;
	  for (auto t : _id2track) {
		  result = std::max(result, t.second->maxTime());
	  }
	  return result;
  }
  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) override {
      std::lock_guard<utils::Locker> lg(_all_tracks_locker);
	  auto tracker = _id2track.find(id);
	  if (tracker != _id2track.end()) {
		  return tracker->second->minMaxTime(id, minResult, maxResult);
	  }
	  return false;
  }
  
  void foreach (const QueryInterval &q, IReaderClb * clbk) override {
      std::lock_guard<utils::Locker> lg(_all_tracks_locker);
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
      std::lock_guard<utils::Locker> lg(_all_tracks_locker);
	  QueryTimePoint local_q({}, q.flag, q.time_point);
	  local_q.ids.resize(1);
	  Id2Meas result;
	  for (auto id : q.ids) {
		  result[id].id = id;
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
      std::lock_guard<utils::Locker> lg(_all_tracks_locker);
	  IdArray local_ids;
      local_ids.resize(1);
	  Id2Meas result;
	  for (auto id : ids) {
		  result[id].id = id;
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

  void setDownLevel(IChunkWriter*down) {
	  _down_level_storage = down;
  }
  Id2Track _id2track;
  MemChunkAllocator _chunk_allocator;
  utils::Locker _all_tracks_locker;
  storage::Settings_ptr _settings;
  IChunkWriter* _down_level_storage;
};

MemStorage::MemStorage(const storage::Settings_ptr &s) : _impl(new MemStorage::Private(s)) {}

MemStorage::~MemStorage() {
  _impl = nullptr;
}

MemStorage::Description MemStorage::description() const{
	return _impl->description();
}

Time MemStorage::minTime() {
  return _impl->minTime();
}

Time MemStorage::maxTime() {
  return _impl->maxTime();
}

bool MemStorage::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                                              dariadb::Time *maxResult) {
  return _impl->minMaxTime(id, minResult, maxResult);
}

void MemStorage::foreach (const QueryInterval &q, IReaderClb * clbk) {
  _impl->foreach (q, clbk);
}

Id2Meas MemStorage::readTimePoint(const QueryTimePoint &q) {
  return _impl->readTimePoint(q);
}

Id2Meas MemStorage::currentValue(const IdArray &ids, const Flag &flag) {
  return _impl->currentValue(ids, flag);
}

append_result MemStorage::append(const Meas &value) {
  return _impl->append(value);
}

void MemStorage::flush() {
  _impl->flush();
}

void MemStorage::setDownLevel(IChunkWriter*_down) {
	_impl->setDownLevel(_down);
}
