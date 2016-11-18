#ifdef MSVC
 #define _SCL_SECURE_NO_WARNINGS //stx::btree
#endif
#include <libdariadb/storage/memstorage.h>
#include <libdariadb/flags.h>
#include <stx/btree_map>
#include <memory>
#include <tuple>
#include <unordered_map>
#include <cstring>
#include <cassert>
#include <set>
#include <shared_mutex>
#include <thread>
#include <condition_variable>
#include <functional>

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

MemChunkAllocator::AllocatedData MemChunkAllocator::allocate() {
  _locker.lock();
  if(_free_list.empty()){
      _locker.unlock();
      return EMPTY;
  }
  auto pos=_free_list.front();
  _free_list.pop_front();
  _allocated++;
  _locker.unlock();
  return AllocatedData(&_headers[pos], &_buffers[pos * _bufferSize], pos);
}

void MemChunkAllocator::free(const MemChunkAllocator::AllocatedData &d) {
	auto header = d.header;
	auto buffer = d.buffer;
	auto position = d.position;
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
  MemChunkAllocator::AllocatedData _a_data;
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
  ~MemChunk() {
  }
};

using MemChunk_Ptr = std::shared_ptr<MemChunk>;
using MemChunkList = std::list<MemChunk_Ptr>;

class MemoryChunkContainer {
public:
	virtual void addChunk(MemChunk_Ptr&c) = 0;
    virtual ~MemoryChunkContainer(){}
};

struct TimeTrack : public IMeasStorage {
  TimeTrack(MemoryChunkContainer*mcc, const Time step, Id meas_id, MemChunkAllocator*allocator) {
	  _allocator = allocator;
    _meas_id = meas_id;
    _step = step;
	_minTime = MAX_TIME;
	_maxTime = MIN_TIME;
	_mcc = mcc;
  }
  ~TimeTrack() {}

  MemChunkAllocator*_allocator;
  Id _meas_id;
  Time _minTime;
  Time _maxTime;
  Time _step;
  MemChunk_Ptr _cur_chunk;
  utils::Locker _locker;
  stx::btree_map<Time, MemChunk_Ptr> _index;
  MemoryChunkContainer*_mcc;

  void updateMinMax(const Meas&value) {
	  _minTime = std::min(_minTime, value.time);
	  _maxTime = std::max(_maxTime, value.time);
  }
  virtual append_result append(const Meas &value) override {
    std::lock_guard<utils::Locker> lg(_locker);
    if (_cur_chunk==nullptr || _cur_chunk->is_full()) {
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
		logger_fatal("engine: memstorage - id:",this->_meas_id, ", can't write to past.");
		return append_result(1, 1);
	}
	updateMinMax(value);
    return append_result(1, 0);
  }

  void flush() override {}
  
  Time minTime() override {
	  return _minTime;
  }

  Time maxTime() override {
	  return _maxTime;
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) override {
	  if (id != this->_meas_id) {
		  return false;
	  }    
	  std::lock_guard<utils::Locker> lg(_locker);
	  *minResult = MAX_TIME;
	  *maxResult = MIN_TIME;
	  for (auto kv : _index) {
		  auto c = kv.second;
		  *minResult = std::min(c->header->minTime, *minResult);
		  *maxResult = std::max(c->header->maxTime, *maxResult);
	  }
	  if (_cur_chunk != nullptr) {
		  *minResult = std::min(_cur_chunk->header->minTime, *minResult);
		  *maxResult = std::max(_cur_chunk->header->maxTime, *maxResult);
	  }
	  return true;
  }

  void foreach (const QueryInterval &q, IReaderClb * clbk) override {
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

			  auto rdr = c->get_reader();

			  while (!rdr->is_end()) {
				  auto v = rdr->readNext();
				  if (utils::inInterval(q.from, q.to, v.time)) {
					  clbk->call(v);
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
	  if (_cur_chunk != nullptr) {
		  auto last = _cur_chunk->header->last;
		  if (last.inFlag(flag)) {
			  result[_meas_id] = last;
			  return result;
		  }
	  }
	  result[_meas_id].flag = Flags::_NO_DATA;
	  return result;
  }

  void rm_chunk(MemChunk *c) {
	  std::lock_guard<utils::Locker> lg(_locker);
    _index.erase(c->header->maxTime);
  
    if (_cur_chunk.get() == c) {
      _cur_chunk = nullptr;
	}
  }

  void rereadMinMax() {
	  if (this->_cur_chunk != nullptr) {
		  this->minMaxTime(this->_cur_chunk->header->first.id, &_minTime, &_maxTime);
	  }
  }

  bool create_new_chunk(const Meas&value) {
	  if (_cur_chunk != nullptr) {
		  this->_index.insert(std::make_pair(_cur_chunk->header->maxTime, _cur_chunk));
	  }
	  auto new_chunk_data = _allocator->allocate();
	  if (new_chunk_data.header == nullptr) {
		  return false;
	  }
	  auto mc = MemChunk_Ptr{ new MemChunk{ new_chunk_data.header,
		  new_chunk_data.buffer,
		  _allocator->_bufferSize, value } };
      mc->_track=this;
      mc->_a_data=new_chunk_data;
	  this->_mcc->addChunk(mc);
	  _cur_chunk = mc;
	  return true;
  }
};

using TimeTrack_ptr = std::shared_ptr<TimeTrack>;
using Id2Track = std::unordered_map<Id, TimeTrack_ptr>;

struct MemStorage::Private : public IMeasStorage, public MemoryChunkContainer {
	Private(const storage::Settings_ptr &s) : _chunk_allocator(s->memory_limit, s->page_chunk_size) {
		_chunks.resize(_chunk_allocator._capacity);
		_stoped = false;
		_down_level_storage = nullptr;
		_settings = s;
		_drop_stop = false;
		_drop_thread = std::thread{ std::bind(&MemStorage::Private::drop_thread_func,this) };
		if (_settings->id_count != 0) {
			_id2track.reserve(_settings->id_count);
		}
	}
	void stop() {
		if (!_stoped) {
			logger_info("engine: stop memstorage.");
			if (this->_down_level_storage != nullptr) {
				this->lock_drop();
				logger_info("engine: memstorage - drop all chunk to disk");
				this->drop_by_limit(1.0, true);
				this->unlock_drop();
			}
			for (size_t i = 0; i < _chunks.size(); ++i) {
				_chunks[i] = nullptr;
			}
			_id2track.clear();

			_drop_stop = true;
			_drop_cond.notify_all();
			_drop_thread.join();

			_stoped = true;
		}
	}
	~Private() {
		stop();
	}

	MemStorage::Description description()const {
		MemStorage::Description result;
        result.allocated = _chunk_allocator._allocated;
        result.allocator_capacity = _chunk_allocator._capacity;
		return result;
	}
  append_result append(const Meas &value) override { 
	  _all_tracks_locker.lock_shared();
	  auto track = _id2track.find(value.id);
	  bool is_created = false;
	  if (track == _id2track.end()) {
		  _all_tracks_locker.unlock_shared();
		  _all_tracks_locker.lock();
		  is_created = true;
		  track = _id2track.find(value.id);
		  if (track == _id2track.end()) {
			  auto new_track = std::make_shared<TimeTrack>(this, Time(0), value.id, &_chunk_allocator);
			  _id2track.insert(std::make_pair(value.id, new_track));
			  _all_tracks_locker.unlock();

			  while (new_track->append(value).writed != 1) {}
			  return append_result(1, 0);
		  }
		  else {
			  _all_tracks_locker.unlock();
		  }
	  }
	  if (!is_created) {
		  _all_tracks_locker.unlock_shared();
	  }
	  while (track->second->append(value).writed != 1) {}
	  return append_result(1, 0);
	  
  }

  void drop_by_limit(float chunk_to_free, bool in_stop) {
    auto cur_chunk_count = this->_chunk_allocator._allocated;
    auto chunks_to_delete = (size_t)(cur_chunk_count * chunk_to_free);
    logger_info("engine: drop ", chunks_to_delete, " chunks of ", cur_chunk_count);
    
	std::vector<Chunk *> all_chunks;
    all_chunks.reserve(cur_chunk_count);
    size_t pos = 0;
    for (auto &c : _chunks) {
      if (c == nullptr) {
        continue;
      }
      if (!in_stop & !c->is_full()) { // not full
        assert(!in_stop);
        assert(!c->is_full());
        continue;
      }
      if (!c->header->is_init) {
        continue;
      }
      all_chunks.push_back(c.get());
      ++pos;
    }

    
	if (_down_level_storage != nullptr) {
		_down_level_storage->appendChunks(all_chunks, pos);
	}
	else {
		logger_info("engine: memstorage _down_level_storage == nullptr");
	}
    std::set<TimeTrack *> updated_tracks;
    for (size_t i = 0; i < pos; ++i) {
      auto c = all_chunks[i];
      auto mc = dynamic_cast<MemChunk *>(c);
      assert(mc != nullptr);

      TimeTrack *track = mc->_track;
      track->rm_chunk(mc);
      updated_tracks.insert(track);

      auto chunk_pos = mc->_a_data.position;
      _chunk_allocator.free(mc->_a_data);
      _chunks[chunk_pos] = nullptr;
    }
    for (auto &t : updated_tracks) {
      t->rereadMinMax();
    }
  }

  void drop_by_limit() {
	  std::lock_guard<std::mutex> lg(_drop_locker);
	  auto current_chunk_count = this->_chunk_allocator._allocated;
	  if (current_chunk_count<this->_chunk_allocator._capacity) {
		  return;
	  }
	  drop_by_limit(_settings->chunks_to_free, false);
  }

  Id2MinMax loadMinMax()override{
      Id2MinMax result;
      std::shared_lock<std::shared_mutex> sl(_all_tracks_locker);
      for (auto t : _id2track) {
          result[t.first].min = t.second->minTime();
          result[t.first].max = t.second->maxTime();
      }
      return result;
  }

  Time minTime() override { 
	  std::shared_lock<std::shared_mutex> sl(_all_tracks_locker);
	  Time result = MAX_TIME;
	  for (auto t : _id2track) {
		  result=std::min(result, t.second->minTime());
	  }
	  return result;
  }
  virtual Time maxTime() override { 
	  std::shared_lock<std::shared_mutex> sl(_all_tracks_locker);
	  Time result = MIN_TIME;
	  for (auto t : _id2track) {
		  result = std::max(result, t.second->maxTime());
	  }
	  return result;
  }
  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) override {
	  std::shared_lock<std::shared_mutex> sl(_all_tracks_locker);
	  auto tracker = _id2track.find(id);
	  if (tracker != _id2track.end()) {
		  return tracker->second->minMaxTime(id, minResult, maxResult);
	  }
	  return false;
  }
  
  void foreach (const QueryInterval &q, IReaderClb * clbk) override {
	  std::shared_lock<std::shared_mutex> sl(_all_tracks_locker);
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
	  std::shared_lock<std::shared_mutex> sl(_all_tracks_locker);
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
	  std::shared_lock<std::shared_mutex> sl(_all_tracks_locker);
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

  void setDownLevel(IChunkContainer*down) {
	  _down_level_storage = down;
  }

  void addChunk(MemChunk_Ptr&chunk) override{
	  assert(chunk->_a_data.position < _chunks.size());
	  assert(chunk->header->is_init);

	  _chunks[chunk->_a_data.position]=chunk;
	  if (is_time_to_drop()) {
		  _drop_cond.notify_all();
	  }
  }
  void lock_drop() {
	  _drop_locker.lock();
  }
  void unlock_drop() {
	  _drop_locker.unlock();
  }

  bool is_time_to_drop() {
	  return (_chunk_allocator._allocated) >= (_chunk_allocator._capacity*_settings->percent_to_drop);
  }

  void drop_thread_func() {
	  while (!_drop_stop) {
		  std::unique_lock<std::mutex> ul(_drop_locker);
		  _drop_cond.wait(ul);
		  if (_drop_stop) {
			  break;
		  }

		  if (!is_time_to_drop()) {
			  continue;
		  }

		  drop_by_limit(_settings->chunks_to_free, false);
	  }

	  logger_info("engine: memstorage dropping stop.");
  }
  Id2Track _id2track;
  MemChunkAllocator _chunk_allocator;
  std::shared_mutex _all_tracks_locker;
  storage::Settings_ptr _settings;
  IChunkContainer* _down_level_storage;
  
  std::vector<MemChunk_Ptr>  _chunks;
  bool _stoped;

  std::thread _drop_thread;
  bool        _drop_stop;
  std::mutex _drop_locker;
  std::condition_variable _drop_cond;
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

void MemStorage::stop() {
	_impl->stop();
}

void MemStorage::setDownLevel(IChunkContainer*_down) {
	_impl->setDownLevel(_down);
}

void MemStorage::lock_drop() {
	_impl->lock_drop();
}
void MemStorage::unlock_drop() {
	_impl->unlock_drop();
}

Id2MinMax MemStorage::loadMinMax(){
    return _impl->loadMinMax();
}
