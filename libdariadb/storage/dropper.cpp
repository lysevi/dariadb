#include <libdariadb/storage/dropper.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/storage/pages/page.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <ctime>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils;
using namespace dariadb::utils::async;

std::shared_ptr<SplitedById> splitById(const MeasArray &ma) {
  std::shared_ptr<SplitedById> result = std::make_shared<SplitedById>();

  std::map<Id, size_t> id2count;
  for (auto v : ma) {
    auto iter = id2count.find(v.id);
    if (iter == id2count.end()) {
      id2count.insert(std::make_pair(v.id, size_t(0)));
    } else {
      id2count[v.id] += 1;
    }
  }

  for (auto i2c : id2count) {
    (*result)[i2c.first].reserve(i2c.second);
  }

  for (auto v : ma) {
    (*result)[v.id].push_back(v);
  }
  return result;
}

Dropper::Dropper(EngineEnvironment_ptr engine_env, PageManager_ptr page_manager,
                 WALManager_ptr wal_manager)
    : _page_manager(page_manager), _wal_manager(wal_manager), _engine_env(engine_env) {
  _active_operations = 0;
  _stop = false;
  _is_stoped = false;
  _settings =
      _engine_env->getResourceObject<Settings>(EngineEnvironment::Resource::SETTINGS);
  _thread_handle = std::thread(&Dropper::drop_wal_internal, this);
}

Dropper::~Dropper() {
  logger("engine", _settings->alias, ": dropper - stop begin.");
  _stop = true;
  while (!_is_stoped) {
    _cond_var.notify_all();
    _dropper_cond_var.notify_all();
  }
  _thread_handle.join();
  logger("engine", _settings->alias, ": dropper - stop end.");
}

DropperDescription Dropper::description() const {
  std::lock_guard<std::mutex> lg(_queue_locker);
  DropperDescription result;
  result.wal = _files_queue.size();
  result.active_works = _active_operations;
  return result;
}

void Dropper::dropWAL(const std::string &fname) {
  std::lock_guard<std::mutex> lg(_queue_locker);
  if (std::count(_files_queue.begin(), _files_queue.end(), fname)) {
    return;
  }
  auto storage_path = _settings->raw_path.value();
  if (utils::fs::path_exists(utils::fs::append_path(storage_path, fname))) {
    _files_queue.emplace_back(fname);
    _cond_var.notify_all();
  }
}

void Dropper::cleanStorage(const std::string &storagePath) {
  logger_info("engine: dropper - check storage ", storagePath);
  auto wals_lst = fs::ls(storagePath, WAL_FILE_EXT);
  auto page_lst = fs::ls(storagePath, PAGE_FILE_EXT);

  for (auto &wal : wals_lst) {
    auto wal_fname = fs::filename(wal);
    for (auto &pagef : page_lst) {
      auto page_fname = fs::filename(pagef);
      if (page_fname == wal_fname) {
        logger_info("engine: fsck wal drop not finished: ", wal_fname);
        logger_info("engine: fsck rm ", pagef);
        PageManager::erase(storagePath, fs::extract_filename(pagef));
      }
    }
  }
}

void Dropper::drop_wal_internal() {
  _is_stoped = false;
  while (!_stop) {
    std::string fname;
    {
      std::unique_lock<std::mutex> ul(_queue_locker);
      _cond_var.wait(ul, [this]() { return !this->_files_queue.empty() || _stop; });
      if (_stop) {
        break;
      }
      if (_files_queue.empty()) {
        continue;
      }
      fname = _files_queue.front();
    }
    while (true) {
      if (_stop) {
        break;
      }
      if (_dropper_lock.try_lock()) {
        break;
      }
    }

    if (_stop) {
      break;
    }
    ENSURE(_active_operations == 0);
    ++_active_operations;
    AsyncTask at = [fname, this](const ThreadInfo &ti) {
      try {
        TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);
        ENSURE(_active_operations == 1);
        this->drop_stage_read(fname);
      } catch (std::exception &ex) {
        THROW_EXCEPTION("Dropper::drop_wal_internal: ", ex.what());
      }
      return false;
    };

    ThreadManager::instance()->post(THREAD_KINDS::DISK_IO,
                                    AT_PRIORITY(at, TASK_PRIORITY::DEFAULT));
    while (_active_operations != size_t(0)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::lock_guard<std::mutex> lg(_queue_locker);
    _files_queue.pop_front();
    _dropper_lock.unlock();
  }
  _is_stoped = true;
}

void Dropper::flush() {
  logger_info("engine", _settings->alias, ": Dropper flush...");
  while (this->description().wal != 0 && _active_operations != size_t(0)) {
    this->_cond_var.notify_all();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  logger_info("engine", _settings->alias, ": Dropper flush end.");
}

void Dropper::drop_stage_read(std::string fname) {
  try {
    ENSURE(_active_operations == 1);
    logger_info("engine", _settings->alias, ": compressing ", fname);
    auto start_time = clock();

    auto storage_path = _settings->raw_path.value();
    auto full_path = fs::append_path(storage_path, fname);

    WALFile_Ptr wal = WALFile::open(_engine_env, full_path, true);
    auto ma = wal->readAll();

    AsyncTask sort_at = [this, start_time, ma, fname](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_KINDS::COMMON, ti.kind);
      drop_stage_sort(fname, start_time, ma);
      return false;
    };

    ThreadManager::instance()->post(THREAD_KINDS::COMMON,
                                    AT_PRIORITY(sort_at, TASK_PRIORITY::DEFAULT));
  } catch (...) {
    --_active_operations;
    throw;
  }
}

void Dropper::drop_stage_sort(std::string fname, clock_t start_time,
                              std::shared_ptr<MeasArray> ma) {
  try {
    ENSURE(_active_operations == 1);
    std::sort(ma->begin(), ma->end(), meas_time_compare_less());
    auto splited = splitById(*ma.get());

    AsyncTask write_at = [this, start_time, fname, splited](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);
      this->drop_stage_compress(fname, start_time, splited);
      return false;
    };
    ThreadManager::instance()->post(THREAD_KINDS::DISK_IO,
                                    AT_PRIORITY(write_at, TASK_PRIORITY::DEFAULT));
  } catch (...) {
    --_active_operations;
    throw;
  }
}

void Dropper::drop_stage_compress(std::string fname, clock_t start_time,
                                  std::shared_ptr<SplitedById> splited) {
  try {
    ENSURE(_active_operations == 1);
    auto without_path = fs::extract_filename(fname);
    auto page_fname = fs::filename(without_path);
    auto pm = _page_manager.get();
    auto am = _wal_manager.get();

    pm->append(page_fname, *splited.get());
    am->erase(fname);
    auto end = clock();
    auto elapsed = double(end - start_time) / CLOCKS_PER_SEC;

    logger_info("engine", _settings->alias, ": compressing ", fname,
                " done. elapsed time - ", elapsed);
    ENSURE(_active_operations == 1);
    --_active_operations;
  } catch (...) {
    --_active_operations;
    throw;
  }
}