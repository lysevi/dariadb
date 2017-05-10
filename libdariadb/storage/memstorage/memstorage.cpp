#ifdef MSVC
#define _SCL_SECURE_NO_WARNINGS // stx::btree
#endif
#include <libdariadb/flags.h>
#include <libdariadb/storage/memstorage/memchunk.h>
#include <libdariadb/storage/memstorage/memstorage.h>
#include <libdariadb/storage/memstorage/timetrack.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <atomic>
#include <cstring>
#include <memory>
#include <set>
#include <shared_mutex>
#include <thread>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils::async;

/**
Map:
  Meas.id -> TimeTrack{ MemChunkList[MemChunk{data}]}
*/
struct MemStorage::Private : public IMeasStorage, public MemoryChunkContainer {
  Private(const EngineEnvironment_ptr &env, size_t id_count)
      : _env(env), _settings(_env->getResourceObject<Settings>(
                       EngineEnvironment::Resource::SETTINGS)) {
    _chunks_count.store(uint64_t());
    allocator_init();
    _stoped = false;
    _down_level_storage = nullptr;
    _disk_storage = nullptr;
    _drop_stop = false;
    _drop_is_stoped = false;
    if (!_settings->is_memory_only_mode) {
      _drop_thread = std::thread{std::bind(&MemStorage::Private::drop_thread_func, this)};
    }
    if (id_count != 0) {
      _id2track.reserve(id_count);
    }
  }
  void stop() {
    if (!_stoped) {
      logger_info("engine", _settings->alias, ": memstorage - begin stoping.");
      if (!_settings->is_memory_only_mode) {
        logger_info("engine", _settings->alias, ": memstorage - stoping drop thread");
        while (!_drop_is_stoped) {
          _drop_stop = true;
          _drop_cond.notify_all();
          utils::sleep_mls(100);
        }
        _drop_thread.join();
      }

      if (this->_down_level_storage != nullptr) {
        logger_info("engine", _settings->alias, ": memstorage - drop all chunk to disk");
        this->drop_by_limit(1.0);
      }
      logger_info("engine", _settings->alias, ": memstorage - memory clear.");
      _id2track.clear();
      ENSURE(_chunk_allocator->_allocated == size_t());

      _chunk_allocator = nullptr;
      ENSURE(_chunk_allocator.use_count() == long(0));
      _stoped = true;
      logger_info("engine", _settings->alias, ": memstorage - stoped.");
    }
  }

  ~Private() { stop(); }

  void allocator_init() {
    IMemoryAllocator *alloc_ptr;
    if (_settings->is_memory_only_mode) {
      alloc_ptr = new UnlimitMemoryAllocator(_settings->chunk_size.value());
    } else {
      alloc_ptr = new RegionChunkAllocator(_settings->memory_limit.value(),
                                           _settings->chunk_size.value());
    }

    _chunk_allocator = IMemoryAllocator_Ptr(alloc_ptr);
  }
  memstorage::Description description() const {
    memstorage::Description result;
    auto region_allocator_ptr =
        dynamic_cast<RegionChunkAllocator *>(_chunk_allocator.get());
    if (region_allocator_ptr != nullptr) {
      result.allocator_capacity = region_allocator_ptr->_capacity;
    }
    result.allocated = _chunk_allocator->_allocated;
    return result;
  }

  Status append(const Meas &value) override {
    TimeTrack_ptr track = nullptr;
    {
      auto iterator = _id2track.find_bucket(value.id);

      if (iterator.v->second == nullptr) {
        auto new_tr =
            std::make_shared<TimeTrack>(this, Time(0), value.id, _chunk_allocator);
        iterator.v->second = new_tr;
        track = new_tr;
      } else {
        track = iterator.v->second;
      }
    }

    while (true) {
      auto st = track->append(value);
      if (st != Status(1) && !_drop_stop) {
        if (_settings->is_memory_only_mode) {
          return st;
        }
        _drop_cond.notify_all();
        continue;
      }
      break;
    }

    if (_disk_storage != nullptr) {
      _disk_storage->append(value);
      track->_max_sync_time = value.time;
    }
    return Status(1);
  }

  void drop_by_limit(float chunk_percent_to_free) {
    logger_info("engine", _settings->alias, ": memstorage - drop_by_limit ",
                chunk_percent_to_free);
    size_t cur_chunk_count = (size_t)_chunks_count.load();
    auto chunks_to_delete = (size_t)(cur_chunk_count * chunk_percent_to_free);

    std::list<MemChunk_Ptr> all_chunks;
    size_t pos = 0;

    std::list<TimeTrack_ptr> tracks;
    auto f = [&tracks](const Id2Track::value_type &v) { tracks.push_back(v.second); };
    _id2track.apply(f);

    for (auto &v : tracks) {
      auto cnt = v->chunks_count();
      if (cnt == size_t()) {
        continue;
      }
      auto percent_from_all = (size_t)((100.0 * cnt) / cur_chunk_count) + 1;

      auto to_drop = (size_t)((chunks_to_delete * percent_from_all) / 100);
      to_drop = to_drop == size_t(0) ? 1 : to_drop;

      auto dropped = v->drop_N(to_drop);

      for (auto d : dropped) {
        all_chunks.push_back(d);
        pos++;
      }
    }

    // std::sort(all_chunks.begin(), all_chunks.end(),
    //          [](const MemChunk_Ptr &left, const MemChunk_Ptr &right) {
    //            return left->header->data_first.time < right->header->data_first.time;
    //          });

    if (pos != 0) {
      logger_info("engine", _settings->alias, ": memstorage - drop begin ", pos,
                  " chunks of ", cur_chunk_count);
      if (_down_level_storage != nullptr) {
        auto chunks_per_page = _settings->max_chunks_per_page.value();

        AsyncTask at = [this, &all_chunks, chunks_per_page](const ThreadInfo &ti) {
          TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);
          this->drop_logic(chunks_per_page, all_chunks);
          return false;
        };

        auto at_res = ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
        at_res->wait();
      } else {
        if (_settings->strategy.value() != STRATEGY::CACHE) {
          logger_info("engine", _settings->alias,
                      ": memstorage _down_level_storage == nullptr");
        }
      }
      logger_info("engine", _settings->alias, ": memstorage - drop end.");
    }
  }

  void drop_logic(size_t, std::list<MemChunk_Ptr> &all_chunks) {
    std::unordered_map<dariadb::Id, std::vector<Chunk *>> c2i;
    std::unordered_map<dariadb::Id, std::vector<MemChunk_Ptr>> c2i_to_drop;
    std::vector<Chunk *> raw_ptrs(all_chunks.size());

    for (auto mc : all_chunks) {
      c2i[mc->header->meas_id].push_back(mc.get());
      c2i_to_drop[mc->header->meas_id].push_back(mc);
    }
    all_chunks.clear();
    for (auto kv : c2i) {
      this->_down_level_storage->appendChunks(kv.second);
      for (auto mc : c2i_to_drop[kv.first]) {
        freeChunk(mc);
      }
      c2i_to_drop[kv.first].clear();
    }
  }

  void dropOld(dariadb::Id id, Time t) {
    logger_info("engine", _settings->alias, ": memstorage - drop old #", id, " [",
                timeutil::to_string(t), "]");
    utils::ElapsedTime et;
    size_t erased = 0;
	TimeTrack_ptr target = nullptr;
	if (_id2track.find(id, &target)) {
		erased += target->drop_Old(t);
	}
    /*auto f = [t, &erased](const Id2Track::value_type &v) {
      erased += v.second->drop_Old(t);
    };

    _id2track.apply(f);

    _chunks_count.fetch_sub(erased);*/
    logger_info("engine", _settings->alias, ": memstorage - drop old complete. erased ",
                erased, " chunks. elapsed ", et.elapsed(), "s");
  }

  Id2Time getSyncMap() {
    Id2Time result;
    result.reserve(_id2track.size());
    auto f = [&result](const Id2Track::value_type &v) {
      result[v.first] = v.second->_max_sync_time;
    };
    _id2track.apply(f);
    return result;
  }

  Id2MinMax_Ptr loadMinMax() override {
    auto result = std::make_shared<Id2MinMax>();
    auto f = [&result](const Id2Track::value_type &v) {
      if (v.second->_cur_chunk == nullptr) {
        return;
      }
      result->insert(v.first, v.second->_min_max);
    };
    _id2track.apply(f);
    return result;
  }

  Time minTime() override {
    Time result = MAX_TIME;
    auto f = [&result](const Id2Track::value_type &v) {
      result = std::min(result, v.second->minTime());
    };
    _id2track.apply(f);
    return result;
  }
  virtual Time maxTime() override {
    Time result = MIN_TIME;
    auto f = [&result](const Id2Track::value_type &v) {
      result = std::max(result, v.second->maxTime());
    };
    _id2track.apply(f);
    return result;
  }

  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) override {
    TimeTrack_ptr output = nullptr;
    if (_id2track.find(id, &output)) {
      return output->minMaxTime(id, minResult, maxResult);
    }
    return false;

    return false;
  }

  Id2Cursor intervalReader(const QueryInterval &q) override {
    Id2Cursor result;
    for (auto id : q.ids) {
      TimeTrack_ptr output = nullptr;
      auto res = _id2track.find(id, &output);
      if (res) {
        auto rdr = output->intervalReader(q);
        if (!rdr.empty()) {
          result[id] = rdr[id];
        }
      }
    }
    return result;
  }

  Statistic stat(const Id id, Time from, Time to) override {
    Statistic result;
    TimeTrack_ptr output = nullptr;
    auto res = _id2track.find(id, &output);
    if (res) {
      result = output->stat(id, from, to);
    }

    return result;
  }

  void foreach (const QueryInterval &q, IReadCallback * clbk) override {
    Id2Cursor readers = intervalReader(q);
    for (auto kv : readers) {
      kv.second->apply(clbk, q);
    }
  }

  virtual Id2Meas readTimePoint(const QueryTimePoint &q) override {
    QueryTimePoint local_q({}, q.flag, q.time_point);
    local_q.ids.resize(1);
    Id2Meas result;
    for (auto id : q.ids) {
      result[id].id = id;
      TimeTrack_ptr output = nullptr;
      auto res = _id2track.find(id, &output);
      if (res) {
        local_q.ids[0] = id;
        auto sub_res = output->readTimePoint(local_q);
        result[id] = sub_res[id];
      } else {
        result[id].flag = FLAGS::_NO_DATA;
      }
    }
    return result;
  }

  virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override {
    IdArray local_ids;
    local_ids.resize(1);
    Id2Meas result;
    for (auto id : ids) {
      result[id].id = id;
      TimeTrack_ptr output = nullptr;
      auto res = _id2track.find(id, &output);
      if (res) {
        local_ids[0] = id;
        auto sub_res = output->currentValue(local_ids, flag);
        result[id] = sub_res[id];
      } else {
        result[id].flag = FLAGS::_NO_DATA;
      }
    }
    return result;
  }

  void flush() override {}

  void setDownLevel(IChunkStorage *down) { _down_level_storage = down; }

  void setDiskStorage(IMeasWriter *_disk) { _disk_storage = _disk; }

  void addChunk(MemChunk_Ptr &) override { _chunks_count.fetch_add(uint64_t(1)); }

  void freeChunk(MemChunk_Ptr &) { _chunks_count.fetch_sub(uint64_t(1)); }

  std::mutex *getLockers() { return &_drop_locker; }

  bool is_time_to_drop() {
    auto region_allocator_ptr =
        dynamic_cast<RegionChunkAllocator *>(_chunk_allocator.get());
    if (region_allocator_ptr != nullptr) {
      return _chunks_count.load() >= (region_allocator_ptr->_capacity *
                                      _settings->percent_when_start_droping.value());
    }
    return false;
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

      drop_by_limit(_settings->percent_to_drop.value());
    }
    _drop_is_stoped = true;
    logger_info("engine", _settings->alias, ": memstorage - dropping thread stoped.");
  }

  Id2Track _id2track;
  EngineEnvironment_ptr _env;
  storage::Settings *_settings;
  IMemoryAllocator_Ptr _chunk_allocator;
  IChunkStorage *_down_level_storage;
  IMeasWriter *_disk_storage;

  std::atomic_size_t _chunks_count;
  bool _stoped;

  std::thread _drop_thread;
  bool _drop_stop;
  bool _drop_is_stoped;
  std::mutex _drop_locker;
  std::condition_variable _drop_cond;
};

MemStorage_ptr MemStorage::create(const EngineEnvironment_ptr &env, size_t id_count) {
  return MemStorage_ptr{new MemStorage(env, id_count)};
}

MemStorage::MemStorage(const EngineEnvironment_ptr &env, size_t id_count)
    : _impl(new MemStorage::Private(env, id_count)) {}

MemStorage::~MemStorage() {
  _impl = nullptr;
}

memstorage::Description MemStorage::description() const {
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

Id2Cursor MemStorage::intervalReader(const QueryInterval &q) {
  return _impl->intervalReader(q);
}

Statistic MemStorage::stat(const Id id, Time from, Time to) {
  return _impl->stat(id, from, to);
}

void MemStorage::foreach (const QueryInterval &q, IReadCallback * clbk) {
  _impl->foreach (q, clbk);
}

Id2Meas MemStorage::readTimePoint(const QueryTimePoint &q) {
  return _impl->readTimePoint(q);
}

Id2Meas MemStorage::currentValue(const IdArray &ids, const Flag &flag) {
  return _impl->currentValue(ids, flag);
}

Status MemStorage::append(const Meas &value) {
  return _impl->append(value);
}

void MemStorage::flush() {
  _impl->flush();
}

void MemStorage::stop() {
  _impl->stop();
}

void MemStorage::setDownLevel(IChunkStorage *_down) {
  _impl->setDownLevel(_down);
}

void MemStorage::setDiskStorage(IMeasWriter *_disk) {
  _impl->setDiskStorage(_disk);
}

std::mutex *MemStorage::getLockers() {
  return _impl->getLockers();
}

Id2MinMax_Ptr MemStorage::loadMinMax() {
  return _impl->loadMinMax();
}

Id2Time MemStorage::getSyncMap() {
  return _impl->getSyncMap();
}

void MemStorage::dropOld(dariadb::Id id, Time t) {
  return _impl->dropOld(id, t);
}