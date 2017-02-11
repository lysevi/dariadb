#include <algorithm>
#include <libdariadb/config.h>
#include <libdariadb/engine.h>
#include <libdariadb/flags.h>
#include <libdariadb/storage/cursors.h>
#include <libdariadb/storage/dropper.h>
#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/storage/memstorage/memstorage.h>
#include <libdariadb/storage/pages/page_manager.h>
#include <libdariadb/storage/subscribe.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/async/locker.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/strings.h>
#include <libdariadb/utils/utils.h>

#include <cstring>
#include <fstream>
#include <iomanip>
#include <shared_mutex>
#include <sstream>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils::async;

class Engine::Private {
public:
  Private(Settings_ptr settings, bool ignore_lock_file) {
    _settings = settings;
    _strategy = _settings->strategy.value();

    _engine_env = EngineEnvironment::create();
    _engine_env->addResource(EngineEnvironment::Resource::SETTINGS,
                             _settings.get());

    logger_info("engine: project version - ", version());
    logger_info("engine: storage format - ", format());
    logger_info("engine: strategy - ", _settings->strategy.value());
    _stoped = false;

    if (!dariadb::utils::fs::path_exists(_settings->storage_path.value())) {
      dariadb::utils::fs::mkdir(_settings->storage_path.value());
      dariadb::utils::fs::mkdir(_settings->raw_path.value());
    }

    lockfile_lock_or_die(ignore_lock_file);

    auto manifest_file_name = utils::fs::append_path(
        _settings->storage_path.value(), MANIFEST_FILE_NAME);

    bool is_new_storage = !utils::fs::file_exists(manifest_file_name);
    if (is_new_storage) {
      logger_info("engine: init new storage.");
    }
    _subscribe_notify.start();
    ThreadManager::Params tpm_params(_settings->thread_pools_params());
    ThreadManager::start(tpm_params);

    _manifest = Manifest::create(_settings);
    _engine_env->addResource(EngineEnvironment::Resource::MANIFEST,
                             _manifest.get());

    if (is_new_storage) {
      _manifest->set_format(std::to_string(format()));
    } else {
      check_storage_version();
      Dropper::cleanStorage(_settings->raw_path.value());
    }

    _page_manager = PageManager::create(_engine_env);

    if (_settings->load_min_max) {
      _min_max_map = _page_manager->loadMinMax();
    }

    if (_strategy != STRATEGY::MEMORY) {
      _wal_manager = WALManager::create(_engine_env);

      _dropper =
          std::make_unique<Dropper>(_engine_env, _page_manager, _wal_manager);
      _wal_manager->setDownlevel(_dropper.get());
      this->_top_level_storage = _wal_manager;
    }

    if (_strategy == STRATEGY::CACHE || _strategy == STRATEGY::MEMORY) {
      _memstorage = MemStorage::create(_engine_env, _min_max_map.size());
      if (_strategy != STRATEGY::CACHE) {
        _memstorage->setDownLevel(_page_manager.get());
      }
      _top_level_storage = _memstorage;
      if (_strategy == STRATEGY::CACHE) {
        _memstorage->setDiskStorage(_wal_manager.get());
      }
    }

    if (_strategy == STRATEGY::WAL) {
      if (_settings->load_min_max) {
        auto amm = _top_level_storage->loadMinMax();
        minmax_append(_min_max_map, amm);
      }
    }

    logger_info("engine: start - OK ");
  }
  ~Private() { this->stop(); }

  void init_storages() {}
  void stop() {
    if (!_stoped) {
      _top_level_storage = nullptr;
      _subscribe_notify.stop();

      this->flush();
      if (_memstorage != nullptr) {
        _memstorage = nullptr;
      }
      _wal_manager = nullptr;
      _page_manager = nullptr;
      _manifest = nullptr;
      _dropper = nullptr;
      _stoped = true;

      ThreadManager::stop();
      lockfile_unlock();
    }
  }

  std::string lockfile_path() {
    return utils::fs::append_path(_settings->storage_path.value(), "lockfile");
  }

  void lockfile_lock_or_die(bool ignore_lock_file) {
    auto lfile = lockfile_path();
    if (utils::fs::path_exists(lfile)) {
      if (!ignore_lock_file) {
        throw_lock_error(_settings->storage_path.value());
      } else {
        lockfile_unlock();
      }
    }
    std::ofstream ofs;
    ofs.open(lfile, std::ios_base::out | std::ios_base::binary);
    if (!ofs.is_open()) {
      throw_lock_error(_settings->storage_path.value());
    }
    ofs << "locked.";
    ofs.close();
  }

  void lockfile_unlock() {
    auto lfile = lockfile_path();
    utils::fs::rm(lfile);
  }

  [[noreturn]] void throw_lock_error(const std::string &lock_file) {
    logger_fatal("engine: storage ", lock_file, " is locked.");
    std::exit(1);
  }

  void check_storage_version() {
    auto current_version = format();
    auto storage_version = std::stoi(_manifest->get_format());
    if (storage_version != current_version) {
      logger_info("engine: openning storage with version - ", storage_version);
      THROW_EXCEPTION("engine: openning storage with greater version.");
    }
  }

  bool try_lock_storage() {
    std::lock_guard<std::mutex> lock(_lock_locker);
    if (_dropper != nullptr && _memstorage != nullptr) {
      auto dl = _dropper->getLocker();
      auto lp = _memstorage->getLockers();
      return std::try_lock(*dl, *lp) == -1;
    }

    if (_dropper == nullptr && _memstorage != nullptr) {
      auto lp = _memstorage->getLockers();
      return lp->try_lock();
    }

    if (_dropper != nullptr && _memstorage == nullptr) {
      auto dl = _dropper->getLocker();
      return dl->try_lock();
    }
    THROW_EXCEPTION("engine: try_lock - bad engine configuration.");
  }

  void lock_storage() {
    std::lock_guard<std::mutex> lock(_lock_locker);
    if (_dropper != nullptr && _memstorage != nullptr) {
      auto dl = _dropper->getLocker();
      auto lp = _memstorage->getLockers();
      std::lock(*dl, *lp);
      return;
    }

    if (_dropper == nullptr && _memstorage != nullptr) {
      auto lp = _memstorage->getLockers();
      lp->lock();
      return;
    }

    if (_dropper != nullptr && _memstorage == nullptr) {
      auto dl = _dropper->getLocker();
      dl->lock();
      return;
    }
  }

  void unlock_storage() {
    if (_dropper != nullptr && _memstorage != nullptr) {
      auto dl = _dropper->getLocker();
      auto lp = _memstorage->getLockers();
      dl->unlock();
      lp->unlock();
      return;
    }

    if (_dropper == nullptr && _memstorage != nullptr) {
      auto lp = _memstorage->getLockers();
      lp->unlock();
      return;
    }

    if (_dropper != nullptr && _memstorage == nullptr) {
      auto dl = _dropper->getLocker();
      dl->unlock();
      return;
    }
  }

  Time minTime() {
    lock_storage();

    auto pmin = _page_manager->minTime();
    if (_strategy == STRATEGY::CACHE) {
      auto amin = this->_wal_manager->minTime();
      pmin = std::min(pmin, amin);
    }
    Time amin = _top_level_storage->minTime();

    unlock_storage();
    return std::min(pmin, amin);
  }

  Time maxTime() {
    lock_storage();

    auto pmax = _page_manager->maxTime();
    if (_strategy == STRATEGY::CACHE) {
      auto amax = this->_wal_manager->maxTime();
      pmax = std::max(pmax, amax);
    }
    Time amax = _top_level_storage->maxTime();

    unlock_storage();
    return std::max(pmax, amax);
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) {
    dariadb::Time subMin1 = dariadb::MAX_TIME, subMax1 = dariadb::MIN_TIME;
    dariadb::Time subMin3 = dariadb::MAX_TIME, subMax3 = dariadb::MIN_TIME;
    bool pr, ar;
    pr = ar = false;
    auto pm = _page_manager.get();
    AsyncTask pm_at = [&pr, &subMin1, &subMax1, id, pm](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_KINDS::COMMON, ti.kind);
      pr = pm->minMaxTime(id, &subMin1, &subMax1);
      return false;
    };
    auto am = _top_level_storage.get();
    AsyncTask am_at = [&ar, &subMin3, &subMax3, id, am](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_KINDS::COMMON, ti.kind);
      ar = am->minMaxTime(id, &subMin3, &subMax3);
      return false;
    };

    lock_storage();

    auto pm_async =
        ThreadManager::instance()->post(THREAD_KINDS::COMMON, AT(pm_at));
    auto am_async =
        ThreadManager::instance()->post(THREAD_KINDS::COMMON, AT(am_at));

    pm_async->wait();
    am_async->wait();

    unlock_storage();

    *minResult = dariadb::MAX_TIME;
    *maxResult = dariadb::MIN_TIME;

    *minResult = std::min(subMin1, subMin3);
    *maxResult = std::max(subMax1, subMax3);
    return pr || ar;
  }

  Id2MinMax loadMinMax() {
    this->lock_storage();

    auto p_mm = this->_page_manager->loadMinMax();
    if (_strategy == STRATEGY::CACHE) {
      auto a_mm = this->_wal_manager->loadMinMax();
      minmax_append(p_mm, a_mm);
    }
    auto t_mm = this->_top_level_storage->loadMinMax();

    this->unlock_storage();

    minmax_append(p_mm, t_mm);
    return p_mm;
  }

  Status append(const Meas &value) {
    Status result{};

    result = _top_level_storage->append(value);

    if (result.writed == 1) {
      _subscribe_notify.on_append(value);
      _min_max_locker.lock();
      auto insert_fres = _min_max_map.find(value.id);
      if (insert_fres == _min_max_map.end()) {
        _min_max_map[value.id].max = value;
      } else {
        insert_fres->second.updateMax(value);
      }
      _min_max_locker.unlock();
    }

    return result;
  }

  void subscribe(const IdArray &ids, const Flag &flag,
                 const ReaderCallback_ptr &clbk) {
    auto new_s = std::make_shared<SubscribeInfo>(ids, flag, clbk);
    _subscribe_notify.add(new_s);
  }

  Id2Meas currentValue(const IdArray &ids, const Flag &flag) {
    lock_storage();

    Id2Meas a_result;
    for (auto kv : _min_max_map) {
      bool ids_check = false;
      if (ids.size() == 0) {
        ids_check = true;
      } else {
        if (std::count(ids.begin(), ids.end(), kv.first)) {
          ids_check = true;
        }
      }
      if (ids_check) {
        if (kv.second.max.inFlag(flag)) {
          a_result.emplace(std::make_pair(kv.first, kv.second.max));
        }
      }
    }

    unlock_storage();
    return a_result;
  }

  void flush() {
    std::lock_guard<std::mutex> lg(_flush_locker);

    if (_wal_manager != nullptr) {
      _wal_manager->flush();
      _dropper->flush();
      _dropper->flush();
    }
    if (_memstorage != nullptr) {
      _memstorage->flush();
    }
    _page_manager->flush();
    this->wait_all_asyncs();
  }

  void wait_all_asyncs() { ThreadManager::instance()->flush(); }

  Engine::Description description() const {
    Engine::Description result;
    memset(&result, 0, sizeof(Description));
    result.wal_count = _wal_manager == nullptr ? 0 : _wal_manager->filesCount();
    result.pages_count = _page_manager->files_count();
    result.active_works = ThreadManager::instance()->active_works();

    if (_dropper != nullptr) {
      result.dropper = _dropper->description();
    }
    if (_memstorage != nullptr) {
      result.memstorage = _memstorage->description();
    }
    return result;
  }

  Id2Cursor interval_readers_from_disk_only(const IdSet &ids,
                                            const QueryInterval &q) {
    if (ids.empty()) {
      return Id2Cursor();
    }
    QueryInterval local_q{IdArray{ids.begin(), ids.end()}, q.flag, q.from,
                          q.to};
    return internal_readers_two_level(local_q, _page_manager, _wal_manager);
  }

  Id2Cursor interval_readers_from_mem_only(const IdSet &ids,
                                           const QueryInterval &q) {
    if (ids.empty()) {
      return Id2Cursor();
    }
    QueryInterval local_q{IdArray{ids.begin(), ids.end()}, q.flag, q.from,
                          q.to};
    return _memstorage->intervalReader(q);
  }

  /// when strategy=CACHE
  Id2Cursor interval_readers_when_cache(const QueryInterval &q) {
    auto memory_mm = _memstorage->loadMinMax();
    auto sync_map = _memstorage->getSyncMap();

    IdSet disk_only;
    IdSet mem_only;
    // id -> dis_q, mem_q;
    std::map<Id, std::pair<QueryInterval, QueryInterval>> queryById;

    for (auto id : q.ids) {
      auto id_mm = memory_mm.find(id);
      if (id_mm == memory_mm.end()) {
        disk_only.insert(id);
      } else {
        if ((id_mm->second.min.time) > q.from) {
          auto min_mem_time = sync_map[id];
          if (min_mem_time <= q.to) {
            QueryInterval disk_q = q;
            disk_q.ids.resize(1);
            disk_q.from = q.from;
            disk_q.to = min_mem_time;
            disk_q.ids[0] = id;

            QueryInterval mem_q = q;
            mem_q.ids.resize(1);
            mem_q.from = min_mem_time + 1;
            mem_q.to = q.to;
            mem_q.ids[0] = id;

            queryById.insert(std::make_pair(id, std::make_pair(disk_q, mem_q)));
          } else {
            disk_only.insert(id);
          }
        } else {
          mem_only.insert(id);
        }
      }
    }

    Id2Cursor result;
    auto disk_only_readers = interval_readers_from_disk_only(disk_only, q);
    auto mem_only_readers = interval_readers_from_mem_only(mem_only, q);

    for (auto id2intervals : queryById) {

      auto disk_q = id2intervals.second.first;
      auto mem_q = id2intervals.second.second;

      auto disk_readers =
          internal_readers_two_level(disk_q, _page_manager, _wal_manager);
      auto mm_readers = _memstorage->intervalReader(mem_q);

      CursorsList readers;
      for (auto kv : mm_readers) {
        readers.push_back(kv.second);
      }
      for (auto kv : disk_readers) {
        readers.push_back(kv.second);
      }

      Cursor_Ptr r_ptr = CursorWrapperFactory::colapseCursors(readers);
      result[id2intervals.first] = r_ptr;
    }

    for (auto kv : disk_only_readers) {
      result[kv.first] = kv.second;
    }
    for (auto kv : mem_only_readers) {
      result[kv.first] = kv.second;
    }
    return result;
  }

  /// when strategy!=CACHEs
  Id2Cursor internal_readers_two_level(const QueryInterval &q,
                                       PageManager_ptr pm, IMeasSource_ptr tm) {
    auto pm_readers = pm->intervalReader(q);
    auto tm_readers = tm->intervalReader(q);

    Id2CursorsList all_readers;
    for (auto kv : tm_readers) {
      all_readers[kv.first].push_back(kv.second);
    }

    for (auto kv : pm_readers) {
      all_readers[kv.first].push_back(kv.second);
    }
    return CursorWrapperFactory::colapseCursors(all_readers);
  }

  Id2Cursor internal_readers_two_level(const QueryInterval &q) {
    return internal_readers_two_level(q, _page_manager, _top_level_storage);
  }

  Id2Cursor intervalReader(const QueryInterval &q) {
    Id2Cursor result;
    AsyncTask pm_at = [q, this, &result](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_KINDS::COMMON, ti.kind);
      if (!try_lock_storage()) {
        return true;
      }

      Id2Cursor r;
      if (this->strategy() == STRATEGY::CACHE) {
        r = interval_readers_when_cache(q);
      } else {
        r = internal_readers_two_level(q);
      }

      this->unlock_storage();

      for (auto kv : r) {
        result[kv.first] = kv.second;
      }
      return false;
    };

    auto at = ThreadManager::instance()->post(THREAD_KINDS::COMMON, AT(pm_at));
    at->wait();
    return result;
  }
  Statistic stat_from_cache(const Id id, Time from, Time to) {
    auto memory_mm = _memstorage->loadMinMax();
    auto sync_map = _memstorage->getSyncMap();

    auto id_mm = memory_mm.find(id);
    if (id_mm == memory_mm.end()) {
      return stat_from_disk(id, from, to);
    } else {
      if ((id_mm->second.min.time) > from) {
        auto min_mem_time = sync_map[id];
        if (min_mem_time <= to) {
          auto disk_stat = stat_from_disk(id, from, min_mem_time);
          auto mem_stat = _memstorage->stat(id, min_mem_time + 1, to);
          disk_stat.update(mem_stat);
          return disk_stat;
        } else {
          return stat_from_disk(id, from, to);
        }
      } else {
        return _memstorage->stat(id, from, to);
      }
    }
  }
  Statistic stat_from_disk(const Id id, Time from, Time to) {
    Statistic result;
    if (_page_manager != nullptr) {
      result.update(_page_manager->stat(id, from, to));
    }

    if (_wal_manager != nullptr) {
      result.update(_wal_manager->stat(id, from, to));
    }
    return result;
  }

  Statistic stat(const Id id, Time from, Time to) {
    Statistic result;

    AsyncTask pm_at = [id, from, to, this, &result](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_KINDS::COMMON, ti.kind);
      if (!try_lock_storage()) {
        return true;
      }
      if (strategy() != STRATEGY::CACHE) {
        result.update(stat_from_disk(id, from, to));

        if (_memstorage != nullptr) {
          result.update(_memstorage->stat(id, from, to));
        }
      } else {
        result.update(stat_from_cache(id, from, to));
      }

      this->unlock_storage();

      return false;
    };
    auto at = ThreadManager::instance()->post(THREAD_KINDS::COMMON, AT(pm_at));
    at->wait();
    return result;
  }

  void foreach (const QueryInterval &q, IReadCallback * clbk) {
    auto r = intervalReader(q);
    for (auto id : q.ids) {
      auto fres = r.find(id);
      if (fres != r.end()) {
        fres->second->apply(clbk, q);
      }
    }
    clbk->is_end();
  }

  void foreach (const QueryTimePoint &q, IReadCallback * clbk) {
    auto values = this->readTimePoint(q);
    for (auto &kv : values) {
      if (clbk->is_canceled()) {
        break;
      }
      clbk->apply(kv.second);
    }
    clbk->is_end();
  }

  MeasList readInterval(const QueryInterval &q) {
    auto a_clbk = std::make_unique<MList_ReaderClb>();
    this->foreach (q, a_clbk.get());
    a_clbk->wait();
    return a_clbk->mlist;
  }

  Id2Meas readTimePoint(const QueryTimePoint &q) {
    Id2Meas result;
    result.reserve(q.ids.size());
    for (auto id : q.ids) {
      result[id].flag = FLAGS::_NO_DATA;
    }

    auto pm = _page_manager.get();
    auto mm = _top_level_storage.get();
    auto am = _wal_manager.get();
    AsyncTask pm_at = [&result, &q, this, pm, mm, am](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_KINDS::COMMON, ti.kind);
      if (!try_lock_storage()) {
        return true;
      }
      for (auto id : q.ids) {

        QueryTimePoint local_q = q;
        local_q.ids.clear();
        local_q.ids.push_back(id);

        dariadb::Time minT, maxT;

        if (mm->minMaxTime(id, &minT, &maxT) &&
            (minT < q.time_point || maxT < q.time_point)) {
          auto subres = mm->readTimePoint(local_q);
          result[id] = subres[id];
        } else {
          bool in_wal_level = false;
          if (this->strategy() == STRATEGY::CACHE) {
            auto subres = am->readTimePoint(local_q);
            auto value = subres[id];
            result[id] = value;
            in_wal_level = value.flag != FLAGS::_NO_DATA;
          }
          if (!in_wal_level) {
            auto subres = _page_manager->valuesBeforeTimePoint(local_q);
            result[id] = subres[id];
          }
        }
      }
      unlock_storage();
      return false;
    };

    auto pm_async =
        ThreadManager::instance()->post(THREAD_KINDS::COMMON, AT(pm_at));
    pm_async->wait();
    return result;
  }

  void drop_part_wals(size_t count) {
    if (_wal_manager != nullptr) {
      logger_info("engine: drop_part_wals ", count);
      _wal_manager->dropClosedFiles(count);
    }
  }

  void compress_all() {
    if (_wal_manager != nullptr) {
      logger_info("engine: compress_all");
      _wal_manager->dropAll();
      this->flush();
    }
  }
  void fsck() {
    logger_info("engine: fsck ", _settings->storage_path.value());
    _page_manager->fsck();
  }

  void eraseOld(const Time &t) {
    logger_info("engine: eraseOld to ", timeutil::to_string(t));
    this->lock_storage();
    _page_manager->eraseOld(t);

    this->unlock_storage();
  }

  STRATEGY strategy() const {
    ENSURE(_strategy == _settings->strategy.value());
    return this->_strategy;
  }

  void repack() {
    this->lock_storage();
    logger_info("engine: repack...");
    _page_manager->repack();
    this->unlock_storage();
  }

  void join(std::list<QueryInterval> queries, Join::Callback *clbk) {
    size_t ids_size = 0;
    CursorsList cursors;
    for (auto &q : queries) {
      auto i2c = this->intervalReader(q);
      ids_size += q.ids.size();
      for (auto id : q.ids) {
        auto cres = i2c.find(id);
        if (cres != i2c.end()) {
          cursors.push_back(cres->second);
        } else {
          cursors.push_back(std::make_shared<EmptyCursor>());
        }
      }
    }
    IdArray ids(ids_size);
    size_t pos = 0;
    for (auto &q : queries) {
      for (auto id : q.ids) {
        ids[pos++] = id;
      }
    }
    ENSURE(ids.size() != size_t(0));
    Join::join(cursors, ids, clbk);
  }

protected:
  std::mutex _flush_locker, _lock_locker;
  SubscribeNotificator _subscribe_notify;

  std::unique_ptr<Dropper> _dropper;
  PageManager_ptr _page_manager;
  WALManager_ptr _wal_manager;
  MemStorage_ptr _memstorage;

  EngineEnvironment_ptr _engine_env;
  Settings_ptr _settings;
  STRATEGY _strategy;
  Manifest_ptr _manifest;
  bool _stoped;

  IMeasStorage_ptr _top_level_storage; // wal or memory storage.

  Id2MinMax _min_max_map;
  std::shared_mutex _min_max_locker;
};

Engine::Engine(Settings_ptr settings, bool ignore_lock_file)
    : _impl{new Engine::Private(settings, ignore_lock_file)} {}

Engine::~Engine() { _impl = nullptr; }

Time Engine::minTime() { return _impl->minTime(); }

Time Engine::maxTime() { return _impl->maxTime(); }
bool Engine::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                        dariadb::Time *maxResult) {
  return _impl->minMaxTime(id, minResult, maxResult);
}

Status Engine::append(const Meas &value) { return _impl->append(value); }

void Engine::subscribe(const IdArray &ids, const Flag &flag,
                       const ReaderCallback_ptr &clbk) {
  _impl->subscribe(ids, flag, clbk);
}

Id2Meas Engine::currentValue(const IdArray &ids, const Flag &flag) {
  return _impl->currentValue(ids, flag);
}

void Engine::flush() { _impl->flush(); }

void Engine::stop() { _impl->stop(); }
Engine::Description Engine::description() const { return _impl->description(); }

void Engine::foreach (const QueryInterval &q, IReadCallback * clbk) {
  return _impl->foreach (q, clbk);
}

Id2Cursor Engine::intervalReader(const QueryInterval &query) {
  return _impl->intervalReader(query);
}

Statistic Engine::stat(const Id id, Time from, Time to) {
  return _impl->stat(id, from, to);
}

void Engine::foreach (const QueryTimePoint &q, IReadCallback * clbk) {
  return _impl->foreach (q, clbk);
}

MeasList Engine::readInterval(const QueryInterval &q) {
  return _impl->readInterval(q);
}

Id2Meas Engine::readTimePoint(const QueryTimePoint &q) {
  return _impl->readTimePoint(q);
}

void Engine::drop_part_wals(size_t count) {
  return _impl->drop_part_wals(count);
}

void Engine::compress_all() { return _impl->compress_all(); }

void Engine::wait_all_asyncs() { return _impl->wait_all_asyncs(); }

void Engine::fsck() { _impl->fsck(); }

void Engine::eraseOld(const Time &t) { return _impl->eraseOld(t); }

void Engine::repack() { _impl->repack(); }

uint16_t Engine::format() { return STORAGE_FORMAT; }

STRATEGY Engine::strategy() const { return _impl->strategy(); }

Id2MinMax Engine::loadMinMax() { return _impl->loadMinMax(); }

std::string Engine::version() { return std::string(PROJECT_VERSION); }

void Engine::join(std::list<QueryInterval> queries,
                  storage::Join::Callback *clbk) {
  return _impl->join(queries, clbk);
}

void dariadb::row2stream(std::ostream &s, const MeasArray &row) {
  for (auto v : row) {
    std::stringstream ss;
    ss << " f:";
    switch (v.flag) {
    case FLAGS::_NO_DATA:
      ss << std::setw(10) << std::left << "_NO_DATA";
      break;
    default:
      ss << std::setw(10) << std::left << std::hex << std::showbase << v.flag;
      break;
    }

    ss << " v:" << std::setw(5) << std::left << std::dec << v.value;
    s << ss.str();
  }
}