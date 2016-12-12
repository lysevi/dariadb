#ifdef MSVC
 #define _SCL_SECURE_NO_WARNINGS //stx::btree
#endif
#include <libdariadb/storage/memstorage/memstorage.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/flags.h>
#include <libdariadb/utils/thread_manager.h>
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
#include <algorithm>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils::async;

/**
Map:
  Meas.id -> TimeTrack{ MemChunkList[MemChunk{data}]}
*/
struct TimeTrack;
struct MemChunk : public ZippedChunk {
  ChunkHeader *index_ptr;
  uint8_t *buffer_ptr;
  MemChunkAllocator::AllocatedData _a_data;
  TimeTrack *_track; /// init in TimeTrack
  size_t  in_disk_count;
  MemChunk(ChunkHeader *index, uint8_t *buffer, size_t size, const Meas& first_m)
      : ZippedChunk(index, buffer, size, first_m) {
    index_ptr = index;
    buffer_ptr = buffer;
	_track = nullptr;
	in_disk_count = 0;
  }
  MemChunk(ChunkHeader *index, uint8_t *buffer) : ZippedChunk(index, buffer) {
    index_ptr = index;
    buffer_ptr = buffer;
	_track = nullptr;
  }
  ~MemChunk() {
  }

  bool already_in_disk()const { //STRATEGY::CACHE, true - if already writed to disk.
	  return in_disk_count == (this->header->count+1); //compressed + first;
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
	_min_max.min.time = MAX_TIME;
	_min_max.max.time = MIN_TIME;
	_max_sync_time = MIN_TIME;
	_mcc = mcc;
  }
  ~TimeTrack() {}

  MemChunkAllocator*_allocator;
  Id _meas_id;
  MeasMinMax _min_max;
  Time _max_sync_time;
  Time _step;
  MemChunk_Ptr _cur_chunk;
  utils::Locker _locker;
  stx::btree_map<Time, MemChunk_Ptr> _index;
  MemoryChunkContainer*_mcc;

  void updateMinMax(const Meas&value) {
	  _min_max.updateMax(value);
	  _min_max.updateMin(value);
  }

  virtual Status  append(const Meas &value) override {
    std::lock_guard<utils::Locker> lg(_locker);
    if (_cur_chunk==nullptr || _cur_chunk->isFull()) {
		if (!create_new_chunk(value)) {
			return Status (0,1);
		}
		else {
			updateMinMax(value);
			return Status (1, 0);
		}
    }
	if (_cur_chunk->header->maxTime < value.time) {
		if (!_cur_chunk->append(value)) {
			if (!create_new_chunk(value)) {
				return Status (0, 1);
			}
			else {
				updateMinMax(value);
				return Status (1, 0);
			}
		}
	}
	else {
		logger_fatal("engine: memstorage - id:",this->_meas_id, ", can't write to past.");
		return Status (1, 1);
	}
	updateMinMax(value);
    return Status (1, 0);
  }

  void flush() override {}
  
  Time minTime() override {
	  return _min_max.min.time;
  }

  Time maxTime() override {
	  return _min_max.max.time;
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

  void foreach_interval_call(const MemChunk_Ptr &c, const QueryInterval &q,
                             IReaderClb *clbk) {
    if (utils::inInterval(c->header->minTime, c->header->maxTime, q.from) ||
        utils::inInterval(c->header->minTime, c->header->maxTime, q.to) ||
        utils::inInterval(q.from, q.to, c->header->minTime) ||
        utils::inInterval(q.from, q.to, c->header->maxTime)) {

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
			  auto rdr = c->getReader();
			  //auto skip = c->in_disk_count;
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
	  
	  if (_cur_chunk!=nullptr && _cur_chunk->header->minTime >= q.time_point && _cur_chunk->header->maxTime <= q.time_point) {
		  auto rdr = _cur_chunk->getReader();
		  //auto skip = _cur_chunk->in_disk_count;
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
	  std::lock_guard<utils::Locker> lg(_locker);
	  _min_max.max.time = MIN_TIME;
	  _min_max.min.time = MIN_TIME;

	  if (_index.size() != 0) {
		  auto c = _index.begin()->second;
		  _min_max.min = c->header->first;
	  }
	  else {
		  if (this->_cur_chunk != nullptr) {
			  _min_max.min = _cur_chunk->header->first;
		  }
	  }

	  if (this->_cur_chunk != nullptr) {		  
		  _min_max.max = _cur_chunk->header->last;
	  }
  }

  bool create_new_chunk(const Meas&value) {
	  if (_cur_chunk != nullptr) {
		  this->_index.insert(std::make_pair(_cur_chunk->header->maxTime, _cur_chunk));
		  _cur_chunk = nullptr;
	  }
	  auto new_chunk_data = _allocator->allocate();
	  if (new_chunk_data.header == nullptr) {
		  return false;
	  }
	  auto mc = MemChunk_Ptr{ new MemChunk{ new_chunk_data.header,
		  new_chunk_data.buffer,
          _allocator->_chunkSize, value } };
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
  Private(const EngineEnvironment_ptr &env, size_t id_count)
      : _env(env), 
	  _settings(_env->getResourceObject<Settings>(EngineEnvironment::Resource::SETTINGS)),
        _chunk_allocator(_settings->memory_limit.value, _settings->chunk_size.value) {
    _chunks.resize(_chunk_allocator._capacity);
    _stoped = false;
    _down_level_storage = nullptr;
    _disk_storage = nullptr;
    _drop_stop = false;
    _drop_thread = std::thread{std::bind(&MemStorage::Private::drop_thread_func, this)};
    if (_settings->strategy.value == STRATEGY::CACHE) {
      logger_info("engine: run memory crawler.");
      _crawler_thread =
          std::thread{std::bind(&MemStorage::Private::crawler_thread_func, this)};
    }
    if (id_count != 0) {
      _id2track.reserve(id_count);
    }
  }
  void stop() {
    if (!_stoped) {
      logger_info("engine: stop memstorage.");
      if (this->_down_level_storage != nullptr) {
        std::lock(_drop_locker, _crawler_locker);
        logger_info("engine: memstorage - drop all chunk to disk");
        this->drop_by_limit(1.0, true);
        _drop_locker.unlock();
        _crawler_locker.unlock();
      }
      for (size_t i = 0; i < _chunks.size(); ++i) {
        _chunks[i] = nullptr;
      }
      _id2track.clear();

      _drop_stop = true;
      _drop_cond.notify_all();
      _drop_thread.join();
      if (_settings->strategy.value == STRATEGY::CACHE) {
        _crawler_cond.notify_all();
        _crawler_thread.join();
      }
      _stoped = true;
    }
  }
  ~Private() { stop(); }

  MemStorage::Description description() const {
    MemStorage::Description result;
    result.allocated = _chunk_allocator._allocated;
    result.allocator_capacity = _chunk_allocator._capacity;
    return result;
  }

  Status append(const Meas &value) override {
    _all_tracks_locker.lock_shared();
    auto track = _id2track.find(value.id);
    TimeTrack_ptr target_track = nullptr;
    if (track == _id2track.end()) {
      _all_tracks_locker.unlock_shared();
      std::lock_guard<std::shared_mutex> lg(_all_tracks_locker);

      track = _id2track.find(value.id);
      if (track == _id2track.end()) { // still not exists.
        target_track =
            std::make_shared<TimeTrack>(this, Time(0), value.id, &_chunk_allocator);
        _id2track.emplace(std::make_pair(value.id, target_track));
      } else {
        target_track = track->second;
      }
    } else {
      target_track = track->second;
      _all_tracks_locker.unlock_shared();
    }
    while (target_track->append(value).writed != 1) {
      _drop_cond.notify_all();
      _crawler_cond.notify_all();
    }
    _crawler_cond.notify_all();
    return Status(1, 0);
  }

  void drop_by_limit(float chunk_percent_to_free, bool in_stop) {
    auto cur_chunk_count = this->_chunk_allocator._allocated;
    auto chunks_to_delete = (size_t)(cur_chunk_count * chunk_percent_to_free);
    
	std::vector<Chunk *> all_chunks;
    all_chunks.reserve(cur_chunk_count);
    size_t pos = 0;

	std::vector<MemChunk_Ptr>  chunks_copy(_chunks.size());
	auto it = std::copy_if(_chunks.begin(), _chunks.end(), chunks_copy.begin(), [](auto c) {return c != nullptr; });
	chunks_copy.resize(std::distance(chunks_copy.begin(), it));

	std::sort(chunks_copy.begin(), chunks_copy.end(),
		[](const MemChunk_Ptr& left, const MemChunk_Ptr& right) {
		return left->header->first.time < right->header->first.time;
		});
    for (auto &c : chunks_copy) {
		if (pos >= chunks_to_delete) {
			break;
		}
      if (c == nullptr) {
        continue;
      }
      if (!in_stop & !c->isFull()) { // not full
        assert(!in_stop);
        assert(!c->isFull());
        continue;
      }
      if (!c->header->is_init) {
        continue;
      }
	  if(_settings->strategy.value == STRATEGY::CACHE && (!c->already_in_disk())){
		  continue;
	  }
      all_chunks.push_back(c.get());
      ++pos;
    }
	if (pos != 0) {
		logger_info("engine: drop ", pos, " chunks of ", cur_chunk_count);
		if (_down_level_storage != nullptr) {
			AsyncTask at = [this, &all_chunks, pos](const ThreadInfo &ti) {
				TKIND_CHECK(THREAD_COMMON_KINDS::DISK_IO, ti.kind);
				this->_down_level_storage->appendChunks(all_chunks, pos);
			};
			auto at_res = ThreadManager::instance()->post(THREAD_COMMON_KINDS::DISK_IO, AT(at));
			at_res->wait();
		}
		else {
			if (_settings->strategy.value != STRATEGY::CACHE) {
				logger_info("engine: memstorage _down_level_storage == nullptr");
			}
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
		logger_info("engine: drop end.");
	}
  }
  
  Id2Time getSyncMap() {
	  Id2Time result;
	  std::shared_lock<std::shared_mutex> sl(_all_tracks_locker);
	  result.reserve(_id2track.size());
	  for (auto t : _id2track) {
		  result[t.first] = t.second->_max_sync_time;
	  }
	  return result;
  }
  
  Id2MinMax loadMinMax()override{
      Id2MinMax result;
      std::shared_lock<std::shared_mutex> sl(_all_tracks_locker);
      for (auto t : _id2track) {
		  if (t.second->_cur_chunk == nullptr) {
			  continue;
		  }
		  result[t.first] = t.second->_min_max;
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
		  if (clbk->is_canceled()) {
			  break;
		  }
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

  void setDiskStorage(IMeasWriter*_disk) {
	  _disk_storage = _disk;
  }

  void addChunk(MemChunk_Ptr&chunk) override{
	  assert(chunk->_a_data.position < _chunks.size());
	  assert(chunk->header->is_init);

	  _chunks[chunk->_a_data.position]=chunk;
	  if (is_time_to_drop()) {
		  _drop_cond.notify_all();
	  }
  }

  std::pair<std::mutex*, std::mutex*> getLockers(){
	  return std::make_pair(&_crawler_locker, &_drop_locker);
  }

  bool is_time_to_drop() {
	  return (_chunk_allocator._allocated) >= (_chunk_allocator._capacity*_settings->percent_when_start_droping.value);
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

      drop_by_limit(_settings->percent_to_drop.value, false);
    }

    logger_info("engine: memstorage - dropping stop.");
  }

  // async. drop values to down-level disk storage;
  void crawler_thread_func() {
    while (!_drop_stop) {
      std::unique_lock<std::mutex> ul(_crawler_locker);
      _crawler_cond.wait(ul);

      if (_disk_storage == nullptr) {
        logger_info("engine: memstorage - disk storage is not set.");
        std::this_thread::yield();
        continue;
      }

      std::vector<MemChunk_Ptr> chunks_copy(_chunks.size());
      auto it =
          std::copy_if(_chunks.begin(), _chunks.end(), chunks_copy.begin(),
                       [](auto c) { return c != nullptr && !c->already_in_disk(); });
      chunks_copy.resize(std::distance(chunks_copy.begin(), it));

      std::sort(chunks_copy.begin(), chunks_copy.end(),
                [](const MemChunk_Ptr &left, const MemChunk_Ptr &right) {
                  return left->header->first.time < right->header->first.time;
                });

      for (auto &c : chunks_copy) {
        auto rdr = c->getReader();
        auto skip = c->in_disk_count;
        int writed = 0;
        Time max_time = c->_track->_max_sync_time;
        while (!rdr->is_end()) {
          auto value = rdr->readNext();
          if (skip != 0) {
            --skip;
          } else {
            auto status = _disk_storage->append(value);
            if (status.writed == 1) {
              max_time = value.time;
              ++writed;
            }
          }
        }
        rdr = nullptr;
        assert(writed <= (c->header->count + 1));
        c->in_disk_count += writed;
        c->_track->_max_sync_time = max_time;
      }
    }
    logger_info("engine: memstorage - crawler stop.");
  }

  Id2Track _id2track;
  EngineEnvironment_ptr _env;
  storage::Settings* _settings;
  MemChunkAllocator _chunk_allocator;
  std::shared_mutex _all_tracks_locker;
  IChunkContainer* _down_level_storage;
  IMeasWriter* _disk_storage;
  
  std::vector<MemChunk_Ptr>  _chunks;
  bool _stoped;

  std::thread _drop_thread;
  std::thread _crawler_thread;
  bool        _drop_stop;
  std::mutex _drop_locker;
  std::mutex _crawler_locker;
  std::condition_variable _drop_cond;
  std::condition_variable _crawler_cond;
};

MemStorage::MemStorage(const EngineEnvironment_ptr &env, size_t id_count) : _impl(new MemStorage::Private(env, id_count)) {}

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

Status  MemStorage::append(const Meas &value) {
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

void MemStorage::setDiskStorage(IMeasWriter *_disk) {
  _impl->setDiskStorage(_disk);
}

std::pair<std::mutex*, std::mutex*> MemStorage::getLockers(){
	return _impl->getLockers();
}

Id2MinMax MemStorage::loadMinMax(){
    return _impl->loadMinMax();
}

Id2Time MemStorage::getSyncMap() {
	return _impl->getSyncMap();
}
