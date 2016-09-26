#include "engine.h"
#include "config.h"
#include "flags.h"
#include "storage/dropper.h"
#include "storage/lock_manager.h"
#include "storage/manifest.h"
#include "storage/page_manager.h"
#include "storage/subscribe.h"
#include "utils/exception.h"
#include "utils/locker.h"
#include "utils/logger.h"
#include "utils/metrics.h"
#include "utils/thread_manager.h"
#include "utils/utils.h"
#include "utils/strings.h"
#include "utils/fs.h"
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
  Private() {
    logger_info("engine: version - ", this->version().to_string());
    logger_info("engine: strategy - ", Options::instance()->strategy);
    bool is_exists = false;
    _stoped = false;
    if (!dariadb::utils::fs::path_exists(Options::instance()->path)) {
      dariadb::utils::fs::mkdir(Options::instance()->path);
    } else {
      is_exists = true;
    }
    _subscribe_notify.start();

    ThreadManager::Params tpm_params(Options::instance()->thread_pools_params());
    ThreadManager::start(tpm_params);
    LockManager::start(LockManager::Params());
    Manifest::start(
        utils::fs::append_path(Options::instance()->path, MANIFEST_FILE_NAME));

    if (is_exists) {
      Dropper::cleanStorage(Options::instance()->path);
    }

    PageManager::start();

    if (!is_exists) {
      Manifest::instance()->set_version(this->version().version);
    } else {
      check_storage_version();
    }

    AOFManager::start();

    _dropper = std::make_unique<Dropper>();

    AOFManager::instance()->set_downlevel(_dropper.get());
    _next_query_id = Id();
  }
  ~Private() { this->stop(); }

  void stop() {
    if (!_stoped) {
      _subscribe_notify.stop();

      this->flush();
	  
	  ThreadManager::stop();
      AOFManager::stop();
      PageManager::stop();
      Manifest::stop();
      LockManager::stop();
      _stoped = true;
    }
  }

  void check_storage_version() {
    auto current_version = this->version().version;
    auto storage_version = Manifest::instance()->get_version();
    if (storage_version != current_version) {
      logger_info("engine: openning storage with version - ", storage_version);
      if (Version::from_string(storage_version) > this->version()) {
        THROW_EXCEPTION("engine: openning storage with greater version.");
      } else {
        logger_info("engine: update storage version to ", current_version);
        Manifest::instance()->set_version(current_version);
      }
    }
  }

  Time minTime() {
    LockManager::instance()->lock(
        LOCK_KIND::READ, {LOCK_OBJECTS::PAGE, LOCK_OBJECTS::AOF});

    auto pmin = PageManager::instance()->minTime();
    auto amin = AOFManager::instance()->minTime();

    LockManager::instance()->unlock(
        {LOCK_OBJECTS::PAGE, LOCK_OBJECTS::AOF});
    return std::min(pmin, amin);
  }

  Time maxTime() {
    LockManager::instance()->lock(
        LOCK_KIND::READ, {LOCK_OBJECTS::PAGE, LOCK_OBJECTS::AOF});

    auto pmax = PageManager::instance()->maxTime();
    auto amax = AOFManager::instance()->maxTime();

    LockManager::instance()->unlock(
        {LOCK_OBJECTS::PAGE, LOCK_OBJECTS::AOF});

    return std::max(pmax, amax);
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult, dariadb::Time *maxResult) {
    TIMECODE_METRICS(ctmd, "minMaxTime", "Engine::minMaxTime");
    dariadb::Time subMin1 = dariadb::MAX_TIME, subMax1 = dariadb::MIN_TIME;
    dariadb::Time subMin3 = dariadb::MAX_TIME, subMax3 = dariadb::MIN_TIME;
    bool pr, ar;
    pr = ar = false;

    AsyncTask pm_at = [&pr, &subMin1, &subMax1, id](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
      pr = PageManager::instance()->minMaxTime(id, &subMin1, &subMax1);

    };
    AsyncTask am_at = [&ar, &subMin3, &subMax3, id](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
      ar = AOFManager::instance()->minMaxTime(id, &subMin3, &subMax3);
    };

    LockManager::instance()->lock(
        LOCK_KIND::READ, {LOCK_OBJECTS::PAGE, LOCK_OBJECTS::AOF});

    auto pm_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::READ, AT(pm_at));
    auto am_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::READ, AT(am_at));

    pm_async->wait();
    am_async->wait();

    LockManager::instance()->unlock(
        {LOCK_OBJECTS::PAGE, LOCK_OBJECTS::AOF});

    *minResult = dariadb::MAX_TIME;
    *maxResult = dariadb::MIN_TIME;

    *minResult = std::min(subMin1, subMin3);
    *maxResult = std::max(subMax1, subMax3);
    return pr  || ar;
  }

  append_result append(const Meas &value) {
    append_result result{};
    result = AOFManager::instance()->append(value);
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
    LockManager::instance()->lock(LOCK_KIND::READ, {LOCK_OBJECTS::AOF});
    auto result = AOFManager::instance()->currentValue(ids, flag);

    LockManager::instance()->unlock({LOCK_OBJECTS::AOF});

    return result;
  }

  void flush() {
    TIMECODE_METRICS(ctmd, "flush", "Engine::flush");
    std::lock_guard<std::mutex> lg(_locker);

    AOFManager::instance()->flush();
    _dropper->flush();
    _dropper->flush();
    PageManager::instance()->flush();
  }

  void wait_all_asyncs() { ThreadManager::instance()->flush(); }

  Engine::QueueSizes queue_size() const {
    QueueSizes result;
    result.aofs_count = AOFManager::instance()->files_count();
    result.pages_count = PageManager::instance()->files_count();
    result.active_works = ThreadManager::instance()->active_works();
    result.dropper_queues = _dropper->queues();
    return result;
  }

  void foreach_internal(const QueryInterval &q, IReaderClb *p_clbk, IReaderClb *a_clbk) {
    TIMECODE_METRICS(ctmd, "foreach", "Engine::internal_foreach");

    AsyncTask pm_at = [&p_clbk, &q](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
      PageManager::instance()->foreach (q, p_clbk);
    };


    AsyncTask am_at = [&a_clbk, &q](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
      AOFManager::instance()->foreach (q, a_clbk);
    };

    LockManager::instance()->lock(
        LOCK_KIND::READ, {LOCK_OBJECTS::PAGE, LOCK_OBJECTS::AOF});

    auto pm_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::READ, AT(pm_at));
    auto am_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::READ, AT(am_at));

    pm_async->wait();
    am_async->wait();

    LockManager::instance()->unlock({LOCK_OBJECTS::PAGE, LOCK_OBJECTS::AOF});
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
    std::unique_ptr<MList_ReaderClb> p_clbk{new MList_ReaderClb};
    std::unique_ptr<MList_ReaderClb> a_clbk{new MList_ReaderClb};
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

    LockManager::instance()->lock(
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

      if (AOFManager::instance()->minMaxTime(id, &minT, &maxT) &&
          (minT < q.time_point || maxT < q.time_point)) {
        auto subres = AOFManager::instance()->readTimePoint(local_q);
        result[id] = subres[id];
        continue;
      } else {
        auto subres = PageManager::instance()->valuesBeforeTimePoint(local_q);
        result[id] = subres[id];
      }
    }

    LockManager::instance()->unlock(
        {LOCK_OBJECTS::PAGE, LOCK_OBJECTS::AOF});
    return result;
  }

  void drop_part_aofs(size_t count) { AOFManager::instance()->drop_closed_files(count); }

  void fsck() {
    logger_info("engine: fsck ", Options::instance()->path);
    PageManager::instance()->fsck();
  }

  void eraseOld(const Time&t) {
	  LockManager::instance()->lock(
		  LOCK_KIND::EXCLUSIVE, { LOCK_OBJECTS::PAGE });
	  PageManager::instance()->eraseOld(t);
	  LockManager::instance()->unlock(LOCK_OBJECTS::PAGE);
  }

  Engine::Version version() {
    Version result;
    result.version = PROJECT_VERSION;
    result.major = PROJECT_VERSION_MAJOR;
    result.minor = PROJECT_VERSION_MINOR;
    result.patch = PROJECT_VERSION_PATCH;
    return result;
  }

protected:
  mutable std::mutex _locker;
  SubscribeNotificator _subscribe_notify;
  Id _next_query_id;
  std::unique_ptr<Dropper> _dropper;
  bool _stoped;
};

Engine::Engine() : _impl{new Engine::Private()} {}

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

Engine::Version Engine::version() {
  return _impl->version();
}
