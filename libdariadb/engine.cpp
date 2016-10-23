#include <libdariadb/engine.h>
#include <libdariadb/config.h>
#include <libdariadb/flags.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/storage/dropper.h>
#include <libdariadb/storage/lock_manager.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/storage/page_manager.h>
#include <libdariadb/storage/subscribe.h>
#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/locker.h>
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/metrics.h>
#include <libdariadb/utils/thread_manager.h>
#include <libdariadb/utils/utils.h>
#include <libdariadb/utils/strings.h>
#include <libdariadb/utils/fs.h>
#include <algorithm>
#include <cassert>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils::async;

std::string Engine::Version::to_string() const {
  return version;
}

Engine::Version Engine::Version::from_string(const std::string &str) {
  std::vector<std::string> elements = utils::strings::split(str, '.');
  assert(elements.size() == 3);

  Engine::Version result;
  result.version = str;
  result.major = (uint16_t)std::stoi(elements[0]);
  result.minor = (uint16_t)std::stoi(elements[1]);
  result.patch = (uint16_t)std::stoi(elements[2]);
  return result;
}

class Engine::Private {
public:
  Private(Settings_ptr settings) {
	  _settings = settings;
	  _engine_env = EngineEnvironment_ptr{ new EngineEnvironment() };
	  _engine_env->addResource(EngineEnvironment::Resource::SETTINGS, _settings.get());

    logger_info("engine: version - ", this->version().to_string());
    logger_info("engine: strategy - ", _settings->strategy);
    bool is_exists = false;
    _stoped = false;
    if (!dariadb::utils::fs::path_exists(_settings->path)) {
      dariadb::utils::fs::mkdir(_settings->path);
    } else {
      is_exists = true;
    }
    _subscribe_notify.start();
    ThreadManager::Params tpm_params(_settings->thread_pools_params());
    ThreadManager::start(tpm_params);
	
	_lock_manager = LockManager_ptr{ new  LockManager(LockManager::Params()) };
	_engine_env->addResource(EngineEnvironment::Resource::LOCK_MANAGER, _lock_manager.get());
    
	_manifest = Manifest_ptr{ new Manifest{ utils::fs::append_path(_settings->path, MANIFEST_FILE_NAME) } };
	_engine_env->addResource(EngineEnvironment::Resource::MANIFEST, _manifest.get());

    if (is_exists) {
      Dropper::cleanStorage(_settings->path);
    }

	_page_manager = PageManager_ptr{ new PageManager(_engine_env) };

    if (utils::fs::path_exists(utils::fs::append_path(settings->path, MANIFEST_FILE_NAME))) {
		_manifest->set_version(this->version().version);
    } else {
      check_storage_version();
    }

	_aof_manager = AOFManager_ptr{ new AOFManager(_engine_env) };

    _dropper = std::make_unique<Dropper>(_engine_env, _page_manager, _aof_manager);

	_aof_manager->set_downlevel(_dropper.get());
    _next_query_id = Id();
  }
  ~Private() { this->stop(); }

  void stop() {
    if (!_stoped) {
      _subscribe_notify.stop();

      this->flush();
	  
	  ThreadManager::stop();
	  _aof_manager = nullptr;
	  _page_manager = nullptr;
	  _manifest = nullptr;
	  _lock_manager = nullptr;
	  _dropper = nullptr;
      _stoped = true;
    }
  }

  void check_storage_version() {
    auto current_version = this->version().version;
    auto storage_version = _manifest->get_version();
    if (storage_version != current_version) {
      logger_info("engine: openning storage with version - ", storage_version);
      if (Version::from_string(storage_version) > this->version()) {
        THROW_EXCEPTION("engine: openning storage with greater version.");
      } else {
        logger_info("engine: update storage version to ", current_version);
		_manifest->set_version(current_version);
      }
    }
  }

  Time minTime() {
    _lock_manager->lock(
        LOCK_KIND::READ, {LOCK_OBJECTS::PAGE, LOCK_OBJECTS::AOF});

    auto pmin = _page_manager->minTime();
    auto amin = _aof_manager->minTime();

    _lock_manager->unlock(
        {LOCK_OBJECTS::PAGE, LOCK_OBJECTS::AOF});
    return std::min(pmin, amin);
  }

  Time maxTime() {
    _lock_manager->lock(
        LOCK_KIND::READ, {LOCK_OBJECTS::PAGE, LOCK_OBJECTS::AOF});

    auto pmax = _page_manager->maxTime();
    auto amax = _aof_manager->maxTime();

    _lock_manager->unlock(
        {LOCK_OBJECTS::PAGE, LOCK_OBJECTS::AOF});

    return std::max(pmax, amax);
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult, dariadb::Time *maxResult) {
    TIMECODE_METRICS(ctmd, "minMaxTime", "Engine::minMaxTime");
    dariadb::Time subMin1 = dariadb::MAX_TIME, subMax1 = dariadb::MIN_TIME;
    dariadb::Time subMin3 = dariadb::MAX_TIME, subMax3 = dariadb::MIN_TIME;
    bool pr, ar;
    pr = ar = false;
	auto pm = _page_manager.get();
    AsyncTask pm_at = [&pr, &subMin1, &subMax1, id, pm](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::COMMON, ti.kind);
      pr = pm->minMaxTime(id, &subMin1, &subMax1);

    };
	auto am = _aof_manager.get();
    AsyncTask am_at = [&ar, &subMin3, &subMax3, id, am](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::COMMON, ti.kind);
      ar = am->minMaxTime(id, &subMin3, &subMax3);
    };

    _lock_manager->lock(
        LOCK_KIND::READ, {LOCK_OBJECTS::PAGE, LOCK_OBJECTS::AOF});

    auto pm_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::COMMON, AT(pm_at));
    auto am_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::COMMON, AT(am_at));

    pm_async->wait();
    am_async->wait();

    _lock_manager->unlock(
        {LOCK_OBJECTS::PAGE, LOCK_OBJECTS::AOF});

    *minResult = dariadb::MAX_TIME;
    *maxResult = dariadb::MIN_TIME;

    *minResult = std::min(subMin1, subMin3);
    *maxResult = std::max(subMax1, subMax3);
    return pr  || ar;
  }

  append_result append(const Meas &value) {
    append_result result{};
    result = _aof_manager->append(value);
    if (result.writed == 1) {
      _subscribe_notify.on_append(value);
    }

    return result;
  }

  void subscribe(const IdArray &ids, const Flag &flag, const ReaderClb_ptr &clbk) {
    auto new_s = std::make_shared<SubscribeInfo>(ids, flag, clbk);
    _subscribe_notify.add(new_s);
  }

  Id2Meas currentValue(const IdArray &ids, const Flag &flag) {
    _lock_manager->lock(LOCK_KIND::READ,
                                  {LOCK_OBJECTS::AOF, LOCK_OBJECTS::PAGE});
    Id2Meas a_result, p_result;
	auto am = _aof_manager.get();
    AsyncTask am_at = [&ids, flag, &a_result, am](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::COMMON, ti.kind);
      a_result = am->currentValue(ids, flag);
    };
	auto pm = _page_manager.get();
    AsyncTask pm_at = [&ids, flag, &p_result, pm](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::COMMON, ti.kind);
      QueryTimePoint qt(ids, flag, dariadb::timeutil::current_time());
      p_result = pm->valuesBeforeTimePoint(qt);
    };

    auto pm_async =
        ThreadManager::instance()->post(THREAD_COMMON_KINDS::COMMON, AT(pm_at));
    auto am_async =
        ThreadManager::instance()->post(THREAD_COMMON_KINDS::COMMON, AT(am_at));

    pm_async->wait();
    am_async->wait();
    _lock_manager->unlock({LOCK_OBJECTS::AOF, LOCK_OBJECTS::PAGE});

    Id2Meas result;
    for (auto &r : a_result) {
      auto fres = p_result.find(r.second.id);
      if ((fres != p_result.end()) && (fres->second.time > r.second.time) &&
          (fres->second.flag != Flags::_NO_DATA)) {
        result.insert(std::make_pair(r.second.id, fres->second));
      } else {
        result.insert(std::make_pair(r.second.id, r.second));
      }
    }
    return result;
  }

  void flush() {
    TIMECODE_METRICS(ctmd, "flush", "Engine::flush");
    std::lock_guard<std::mutex> lg(_locker);

	_aof_manager->flush();
    _dropper->flush();
    _dropper->flush();
	_page_manager->flush();
  }

  void wait_all_asyncs() { ThreadManager::instance()->flush(); }

  Engine::QueueSizes queue_size() const {
    QueueSizes result;
    result.aofs_count = _aof_manager->files_count();
    result.pages_count = _page_manager->files_count();
    result.active_works = ThreadManager::instance()->active_works();
    result.dropper_queues = _dropper->queues();
    return result;
  }

  void foreach_internal(const QueryInterval &q, IReaderClb *p_clbk, IReaderClb *a_clbk) {
    TIMECODE_METRICS(ctmd, "foreach", "Engine::internal_foreach");
	auto pm = _page_manager.get();
    AsyncTask pm_at = [&p_clbk, &q, pm](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::COMMON, ti.kind);
      pm->foreach (q, p_clbk);
    };

	auto am = _aof_manager.get();
    AsyncTask am_at = [&a_clbk, &q, am](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::COMMON, ti.kind);
	  am->foreach (q, a_clbk);
    };

    _lock_manager->lock(
        LOCK_KIND::READ, {LOCK_OBJECTS::PAGE, LOCK_OBJECTS::AOF});

    auto pm_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::COMMON, AT(pm_at));
    auto am_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::COMMON, AT(am_at));

    pm_async->wait();
    am_async->wait();

    _lock_manager->unlock({LOCK_OBJECTS::PAGE, LOCK_OBJECTS::AOF});
    a_clbk->is_end();
  }

  // Inherited via MeasStorage
  void foreach (const QueryInterval &q, IReaderClb * clbk) {
    return foreach_internal(q, clbk, clbk);
  }

  void foreach (const QueryTimePoint &q, IReaderClb * clbk) {
    auto values = this->readTimePoint(q);
    for (auto &kv : values) {
      clbk->call(kv.second);
    }
    clbk->is_end();
  }

  void mlist2mset(MeasList &mlist, Id2MSet &sub_result) {
    for (auto m : mlist) {
      if (m.flag == Flags::_NO_DATA) {
        continue;
      }
      sub_result[m.id].insert(m);
    }
  }

  MeasList readInterval(const QueryInterval &q) {
    TIMECODE_METRICS(ctmd, "readInterval", "Engine::readInterval");
    auto p_clbk= std::make_unique<MList_ReaderClb>();
    auto a_clbk= std::make_unique<MList_ReaderClb>();;
    this->foreach_internal(q, p_clbk.get(), a_clbk.get());
    Id2MSet sub_result;

    mlist2mset(p_clbk->mlist, sub_result);
    mlist2mset(a_clbk->mlist, sub_result);

    MeasList result;
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

  Id2Meas readTimePoint(const QueryTimePoint &q) {
    TIMECODE_METRICS(ctmd, "readTimePoint", "Engine::readTimePoint");

    _lock_manager->lock(
        LOCK_KIND::READ, {LOCK_OBJECTS::PAGE, LOCK_OBJECTS::AOF});

    Id2Meas result;
    result.reserve(q.ids.size());
    for (auto id : q.ids) {
      result[id].flag = Flags::_NO_DATA;
    }

    for (auto id : q.ids) {
      dariadb::Time minT, maxT;
      QueryTimePoint local_q = q;
      local_q.ids.clear();
      local_q.ids.push_back(id);

      if (_aof_manager->minMaxTime(id, &minT, &maxT) &&
          (minT < q.time_point || maxT < q.time_point)) {
        auto subres = _aof_manager->readTimePoint(local_q);
        result[id] = subres[id];
        continue;
      } else {
        auto subres = _page_manager->valuesBeforeTimePoint(local_q);
        result[id] = subres[id];
      }
    }

    _lock_manager->unlock(
        {LOCK_OBJECTS::PAGE, LOCK_OBJECTS::AOF});
    return result;
  }

  void drop_part_aofs(size_t count) { _aof_manager->drop_closed_files(count); }

  void fsck() {
    logger_info("engine: fsck ", _settings->path);
	_page_manager->fsck();
  }

  void eraseOld(const Time&t) {
	  _lock_manager->lock(
		  LOCK_KIND::EXCLUSIVE, { LOCK_OBJECTS::PAGE });
	  _page_manager->eraseOld(t);
	  _lock_manager->unlock(LOCK_OBJECTS::PAGE);
  }

  Engine::Version version() {
    Version result;
    result.version = PROJECT_VERSION;
    result.major = PROJECT_VERSION_MAJOR;
    result.minor = PROJECT_VERSION_MINOR;
    result.patch = PROJECT_VERSION_PATCH;
    return result;
  }

  STRATEGY strategy()const{
      return this->_settings->strategy;
  }


  void compactTo(uint32_t pagesCount) {
	  _lock_manager->lock(
		  LOCK_KIND::EXCLUSIVE, { LOCK_OBJECTS::PAGE });
      logger_info("engine: compacting to ", pagesCount);
	  _page_manager->compactTo(pagesCount);
	  _lock_manager->unlock(LOCK_OBJECTS::PAGE);
  }

  void compactbyTime(Time from, Time to) {
	  _lock_manager->lock(
		  LOCK_KIND::EXCLUSIVE, { LOCK_OBJECTS::PAGE });
      logger_info("engine: compacting by time ", timeutil::to_string(from), " ",timeutil::to_string(to));
	  _page_manager->compactbyTime(from, to);
	  _lock_manager->unlock(LOCK_OBJECTS::PAGE);
  }
protected:
  mutable std::mutex _locker;
  SubscribeNotificator _subscribe_notify;
  Id _next_query_id;
  std::unique_ptr<Dropper> _dropper;
  LockManager_ptr _lock_manager;
  PageManager_ptr _page_manager;
  AOFManager_ptr _aof_manager;
  EngineEnvironment_ptr _engine_env;
  Settings_ptr _settings;
  Manifest_ptr _manifest;
  bool _stoped;
};

Engine::Engine(Settings_ptr settings) : _impl{new Engine::Private(settings)} {}

Engine::~Engine() {
  _impl = nullptr;
}

Time Engine::minTime() {
  return _impl->minTime();
}

Time Engine::maxTime() {
  return _impl->maxTime();
}
bool Engine::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                        dariadb::Time *maxResult) {
  return _impl->minMaxTime(id, minResult, maxResult);
}

append_result Engine::append(const Meas &value) {
  return _impl->append(value);
}

void Engine::subscribe(const IdArray &ids, const Flag &flag, const ReaderClb_ptr &clbk) {
  _impl->subscribe(ids, flag, clbk);
}

Id2Meas Engine::currentValue(const IdArray &ids, const Flag &flag) {
  return _impl->currentValue(ids, flag);
}

void Engine::flush() {
  _impl->flush();
}

void Engine::stop() {
  _impl->stop();
}
Engine::QueueSizes Engine::queue_size() const {
  return _impl->queue_size();
}

void Engine::foreach (const QueryInterval &q, IReaderClb * clbk) {
  return _impl->foreach (q, clbk);
}

void Engine::foreach (const QueryTimePoint &q, IReaderClb * clbk) {
  return _impl->foreach (q, clbk);
}

MeasList Engine::readInterval(const QueryInterval &q) {
  return _impl->readInterval(q);
}

Id2Meas Engine::readTimePoint(const QueryTimePoint &q) {
  return _impl->readTimePoint(q);
}

void Engine::drop_part_aofs(size_t count) {
  return _impl->drop_part_aofs(count);
}

void Engine::wait_all_asyncs() {
  return _impl->wait_all_asyncs();
}

void Engine::fsck() {
  _impl->fsck();
}

void Engine::eraseOld(const Time&t) {
	return _impl->eraseOld(t);
}

void Engine::compactTo(uint32_t pagesCount) {
	_impl->compactTo(pagesCount);
}

void Engine::compactbyTime(Time from, Time to) {
	_impl->compactbyTime(from,to);
}
Engine::Version Engine::version() {
	return _impl->version();
}

STRATEGY Engine::strategy()const{
    return _impl->strategy();
}
