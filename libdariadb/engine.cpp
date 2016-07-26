#include "engine.h"
#include "flags.h"
#include "storage/bloom_filter.h"
#include "storage/capacitor_manager.h"
#include "storage/lock_manager.h"
#include "storage/manifest.h"
#include "storage/page_manager.h"
#include "storage/subscribe.h"
#include "utils/exception.h"
#include "utils/locker.h"
#include "utils/logger.h"
#include "utils/metrics.h"
#include "utils/thread_manager.h"
#include <algorithm>
#include <cassert>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils::async;

class AofDropper : public dariadb::storage::IAofFileDropper {
  std::string _storage_path;

public:
  AofDropper(std::string storage_path) { _storage_path = storage_path; }
  static void drop(const AOFile_Ptr aof, const std::string &fname,
                   const std::string &storage_path) {
    auto target_name = utils::fs::filename(fname) + CAP_FILE_EXT;
    if (dariadb::utils::fs::path_exists(
            utils::fs::append_path(storage_path, target_name))) {
      return;
    }

    auto ma = aof->readAll();
    std::sort(ma.begin(), ma.end(), meas_time_compare_less());
    CapacitorManager::instance()->append(target_name, ma);
    Manifest::instance()->aof_rm(fname);
    utils::fs::rm(utils::fs::append_path(storage_path, fname));
  }
  void drop(const AOFile_Ptr aof, const std::string fname) override {
    AsyncTask at = [fname, aof, this](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::DROP, ti.kind);
      TIMECODE_METRICS(ctmd, "drop", "AofDropper::drop");
      LockManager::instance()->lock(LockKind::EXCLUSIVE, LockObjects::DROP_AOF);
      AofDropper::drop(aof, fname, _storage_path);
      LockManager::instance()->unlock(LockObjects::DROP_AOF);
    };

    ThreadManager::instance()->post(THREAD_COMMON_KINDS::DROP, AT(at));
  }

  // on start, rm COLA files with name exists AOF file.
  static void cleanStorage(std::string storagePath) {
    auto aofs_lst = utils::fs::ls(storagePath, AOF_FILE_EXT);
    auto caps_lst = utils::fs::ls(storagePath, CAP_FILE_EXT);

    for (auto &aof : aofs_lst) {
      auto aof_fname = utils::fs::filename(aof);
      for (auto &capf : caps_lst) {
        auto cap_fname = utils::fs::filename(capf);
        if (cap_fname == aof_fname) {
          logger_info("fsck: aof drop not finished: " << aof_fname);
          logger_info("fsck: rm " << capf);
          utils::fs::rm(capf);
          Manifest::instance()->cola_rm(utils::fs::extract_filename(capf));
        }
      }
    }
  }
};

class CapDrooper : public CapacitorManager::ICapDropper {
public:
  void drop(const std::string &fname) override {
    AsyncTask at = [fname, this](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::DROP, ti.kind);
      TIMECODE_METRICS(ctmd, "drop", "CapDrooper::drop");

      LockManager::instance()->lock(LockKind::EXCLUSIVE, LockObjects::DROP_CAP);
      auto p = Capacitor::Params(0, "");
      auto cap = Capacitor_Ptr{new Capacitor{p, fname, false}};

      auto without_path =  utils::fs::extract_filename(fname);
      auto page_fname =  utils::fs::filename(without_path);
      auto all=cap->readAll();
      assert(all.size()==cap->size());
      PageManager::instance()->append(page_fname,all);
      Manifest::instance()->cola_rm(without_path);
      cap=nullptr;
      utils::fs::rm(fname);

      LockManager::instance()->unlock(LockObjects::DROP_CAP);
    };
    ThreadManager::instance()->post(THREAD_COMMON_KINDS::DROP, AT(at));
  }

  // on start, rm PAGE files with name exists CAP file.
  static void cleanStorage(std::string storagePath) {
	  auto caps_lst = utils::fs::ls(storagePath, CAP_FILE_EXT);
	  auto page_lst = utils::fs::ls(storagePath, ".page");
	  for (auto &cap : caps_lst) {
		  auto cap_fname = utils::fs::filename(cap);
		  for (auto &page : page_lst) {
			  auto page_fname = utils::fs::filename(page);
			  if (cap_fname == page_fname) {
				  logger_info("fsck: aof drop not finished: " << page_fname);
				  logger_info("fsck: rm " << page_fname);
				  utils::fs::rm(page_fname);
				  Manifest::instance()->page_rm(utils::fs::extract_filename(page));
			  }
		  }
	  }
  }
};

class Engine::Private {
public:
  Private(storage::AOFManager::Params &aof_params,
          const PageManager::Params &page_storage_params,
          dariadb::storage::CapacitorManager::Params &cap_params)
      : _page_manager_params(page_storage_params), _cap_params(cap_params) {
    bool is_exists = false;
    _stoped = false;
    if (!dariadb::utils::fs::path_exists(aof_params.path)) {
      dariadb::utils::fs::mkdir(aof_params.path);
    } else {
      is_exists = true;
    }
    _subscribe_notify.start();

    ThreadManager::Params tpm_params(THREAD_MANAGER_COMMON_PARAMS);
    ThreadManager::start(tpm_params);
    LockManager::start(LockManager::Params());
    Manifest::start(utils::fs::append_path(aof_params.path, MANIFEST_FILE_NAME));

    if (is_exists) {
      AofDropper::cleanStorage(aof_params.path);
    }

    PageManager::start(_page_manager_params);
    if (is_exists) {
      CapDrooper::cleanStorage(aof_params.path);
    }
    AOFManager::start(aof_params);
    CapacitorManager::start(_cap_params);
    if (is_exists) {
      CapacitorManager::instance()->fsck();
      PageManager::instance()->fsck();
    }

    _aof_dropper = std::unique_ptr<AofDropper>(new AofDropper(aof_params.path));
    _cap_dropper = std::unique_ptr<CapDrooper>(new CapDrooper());

    AOFManager::instance()->set_downlevel(_aof_dropper.get());
    CapacitorManager::instance()->set_downlevel(_cap_dropper.get());
    _next_query_id = Id();
  }
  ~Private() { this->stop(); }

  void stop() {
    if (!_stoped) {
      _subscribe_notify.stop();

      this->flush();

      ThreadManager::stop();
      AOFManager::stop();
      CapacitorManager::stop();
      PageManager::stop();
      Manifest::stop();
      LockManager::stop();

      _stoped = true;
    }
  }

  Time minTime() {
    std::lock_guard<std::mutex> lg(_locker);
    auto pmin = PageManager::instance()->minTime();
    auto cmin = CapacitorManager::instance()->minTime();
    auto amin = AOFManager::instance()->minTime();
    return std::min(std::min(pmin, cmin), amin);
  }

  Time maxTime() {
    std::lock_guard<std::mutex> lg(_locker);
    auto pmax = PageManager::instance()->maxTime();
    auto cmax = CapacitorManager::instance()->maxTime();
    auto amax = AOFManager::instance()->maxTime();
    return std::max(std::max(pmax, cmax), amax);
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult, dariadb::Time *maxResult) {
    TIMECODE_METRICS(ctmd, "minMaxTime", "Engine::minMaxTime");
    std::lock_guard<std::mutex> lg(_locker);
    dariadb::Time subMin1 = dariadb::MAX_TIME, subMax1 = dariadb::MIN_TIME;
    dariadb::Time subMin2 = dariadb::MAX_TIME, subMax2 = dariadb::MIN_TIME;
    dariadb::Time subMin3 = dariadb::MAX_TIME, subMax3 = dariadb::MIN_TIME;
    bool pr, mr, ar;
    pr = mr = ar = false;

    AsyncTask pm_at = [&pr, &subMin1, &subMax1, id](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
      pr = PageManager::instance()->minMaxTime(id, &subMin1, &subMax1);

    };
    AsyncTask cm_at = [&mr, &subMin2, &subMax2, id](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
      mr = CapacitorManager::instance()->minMaxTime(id, &subMin2, &subMax2);
    };
    AsyncTask am_at = [&ar, &subMin3, &subMax3, id](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
      ar = AOFManager::instance()->minMaxTime(id, &subMin3, &subMax3);
    };

    LockManager::instance()->lock(
        LockKind::READ, {LockObjects::PAGE, LockObjects::CAP, LockObjects::AOF});

    auto pm_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::READ, AT(pm_at));
    auto cm_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::READ, AT(cm_at));
    auto am_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::READ, AT(am_at));

    pm_async->wait();
    cm_async->wait();
    am_async->wait();

    LockManager::instance()->unlock(
        {LockObjects::PAGE, LockObjects::CAP, LockObjects::AOF});

    *minResult = dariadb::MAX_TIME;
    *maxResult = dariadb::MIN_TIME;

    *minResult = std::min(subMin1, subMin2);
    *minResult = std::min(*minResult, subMin3);
    *maxResult = std::max(subMax1, subMax2);
    *maxResult = std::max(*maxResult, subMax3);
    return pr || mr || ar;
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

  Meas::Id2Meas currentValue(const IdArray &ids, const Flag &flag) {
    LockManager::instance()->lock(LockKind::READ, LockObjects::AOF);
    auto result = AOFManager::instance()->currentValue(ids, flag);
    LockManager::instance()->unlock(LockObjects::AOF);
    return result;
  }

  void flush() {
    TIMECODE_METRICS(ctmd, "flush", "Engine::flush");
    std::lock_guard<std::mutex> lg(_locker);
    AOFManager::instance()->flush();
    CapacitorManager::instance()->flush();
    PageManager::instance()->flush();
  }

  void wait_all_asyncs() { ThreadManager::instance()->flush(); }

  Engine::QueueSizes queue_size() const {
    QueueSizes result;
    result.aofs_count = AOFManager::instance()->files_count();
    result.pages_count = PageManager::instance()->files_count();
    result.cola_count = CapacitorManager::instance()->files_count();
    result.active_works = ThreadManager::instance()->active_works();
    return result;
  }

  // Inherited via MeasStorage
  void foreach (const QueryInterval &q, IReaderClb * clbk) {
    TIMECODE_METRICS(ctmd, "foreach", "Engine::foreach");

    AsyncTask pm_at = [&clbk, &q](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
      auto all_chunkLinks = PageManager::instance()->chunksByIterval(q);

      PageManager::instance()->readLinks(q, all_chunkLinks, clbk);
      all_chunkLinks.clear();
    };

    AsyncTask cm_at = [&clbk, &q](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
      CapacitorManager::instance()->foreach (q, clbk);
    };

    AsyncTask am_at = [&clbk, &q](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
      AOFManager::instance()->foreach (q, clbk);
    };

    LockManager::instance()->lock(
        LockKind::READ, {LockObjects::PAGE, LockObjects::CAP, LockObjects::AOF});

    auto pm_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::READ, AT(pm_at));
    auto cm_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::READ, AT(cm_at));
    auto am_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::READ, AT(am_at));

    pm_async->wait();
    cm_async->wait();
    am_async->wait();

    LockManager::instance()->unlock(
        {LockObjects::PAGE, LockObjects::CAP, LockObjects::AOF});
  }

  Meas::MeasList readInterval(const QueryInterval &q) {
    TIMECODE_METRICS(ctmd, "readInterval", "Engine::readInterval");
    std::unique_ptr<MList_ReaderClb> clbk{new MList_ReaderClb};
    this->foreach (q, clbk.get());
    std::map<dariadb::Id, std::set<Meas, meas_time_compare_less>> sub_result;
    for (auto m : clbk->mlist) {
      if (m.flag == Flags::_NO_DATA) {
        continue;
      }
      sub_result[m.id].insert(m);
    }
    Meas::MeasList result;
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

  Meas::Id2Meas readInTimePoint(const QueryTimePoint &q) {
    TIMECODE_METRICS(ctmd, "readInTimePoint", "Engine::readInTimePoint");

    LockManager::instance()->lock(
        LockKind::READ, {LockObjects::PAGE, LockObjects::CAP, LockObjects::AOF});

    Meas::Id2Meas result;
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
        auto subres = AOFManager::instance()->readInTimePoint(local_q);
        result[id] = subres[id];
      } else {
        if (CapacitorManager::instance()->minMaxTime(id, &minT, &maxT) &&
            (utils::inInterval(minT, maxT, q.time_point))) {
          auto subres = CapacitorManager::instance()->readInTimePoint(local_q);
          result[id] = subres[id];
        } else {
          auto subres = PageManager::instance()->valuesBeforeTimePoint(local_q);
          result[id] = subres[id];
        }
      }
    }

    LockManager::instance()->unlock(
        {LockObjects::PAGE, LockObjects::CAP, LockObjects::AOF});
    return result;
  }

  Id load(const QueryInterval &qi) {
    std::lock_guard<std::mutex> lg(_locker);
    Id result = _next_query_id++;

    auto vals = this->readInterval(qi);
    _load_results.insert(std::make_pair(result, std::make_shared<Meas::MeasList>(vals)));
    return result;
  }

  Id load(const QueryTimePoint &qt) {
    std::lock_guard<std::mutex> lg(_locker);
    Id result = _next_query_id++;

    auto id2m = this->readInTimePoint(qt);
    _load_results.insert(std::make_pair(result, std::make_shared<Meas::MeasList>()));
    for (auto &kv : id2m) {
      _load_results[result]->push_back(kv.second);
    }
    return result;
  }

  Meas::MeasList getResult(Id id) {
    Meas::MeasList result;
    auto fres = _load_results.find(id);
    if (fres == _load_results.end()) {
      return result;
    } else {
      for (auto &v : *(fres->second)) {
        result.push_back(v);
      }
      return result;
    }
  }

  void drop_part_caps(size_t count) {
    CapacitorManager::instance()->drop_closed_files(count);
  }

protected:
  storage::PageManager::Params _page_manager_params;
  dariadb::storage::CapacitorManager::Params _cap_params;

  mutable std::mutex _locker;
  SubscribeNotificator _subscribe_notify;
  Id _next_query_id;
  std::unordered_map<Id, std::shared_ptr<Meas::MeasList>> _load_results;
  std::unique_ptr<AofDropper> _aof_dropper;
  std::unique_ptr<CapDrooper> _cap_dropper;
  bool _stoped;
};

Engine::Engine(storage::AOFManager::Params aof_params,
               storage::PageManager::Params page_manager_params,
               dariadb::storage::CapacitorManager::Params cap_params)
    : _impl{new Engine::Private(aof_params, page_manager_params, cap_params)} {}

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

Id dariadb::storage::Engine::load(const QueryInterval &qi) {
  return _impl->load(qi);
}

Id dariadb::storage::Engine::load(const QueryTimePoint &qt) {
  return _impl->load(qt);
}

Meas::MeasList dariadb::storage::Engine::getResult(Id id) {
  return _impl->getResult(id);
}

append_result Engine::append(const Meas &value) {
  return _impl->append(value);
}

void Engine::subscribe(const IdArray &ids, const Flag &flag, const ReaderClb_ptr &clbk) {
  _impl->subscribe(ids, flag, clbk);
}

Meas::Id2Meas Engine::currentValue(const IdArray &ids, const Flag &flag) {
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

Meas::MeasList Engine::readInterval(const QueryInterval &q) {
  return _impl->readInterval(q);
}

Meas::Id2Meas Engine::readInTimePoint(const QueryTimePoint &q) {
  return _impl->readInTimePoint(q);
}

void Engine::drop_part_caps(size_t count) {
  return _impl->drop_part_caps(count);
}

void Engine::wait_all_asyncs() {
  return _impl->wait_all_asyncs();
}
