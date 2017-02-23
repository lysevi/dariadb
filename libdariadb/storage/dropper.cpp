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

Dropper::Dropper(EngineEnvironment_ptr engine_env, PageManager_ptr page_manager,
                 WALManager_ptr wal_manager)
    : _page_manager(page_manager), _wal_manager(wal_manager), _engine_env(engine_env) {
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
  }
  _thread_handle.join();
  logger("engine", _settings->alias, ": dropper - stop end.");
}

DropperDescription Dropper::description() const {
  std::lock_guard<std::mutex> lg(_queue_locker);
  DropperDescription result;
  result.wal = _files_queue.size();
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
  auto env = _engine_env;
  auto sett = _settings;
  _is_stoped = false;
  while (!_stop) {
    std::string fname;
    {
      std::unique_lock<std::mutex> ul(_queue_locker);
      _cond_var.wait(ul, [this]() { return !this->_files_queue.empty() || _stop; });
      if (_stop) {
        break;
      }
      fname = _files_queue.front();
      _files_queue.pop_front();
    }
    while (!this->_dropper_lock.try_lock() || _stop) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (_stop) {
      break;
    }
    AsyncTask at = [fname, this, env, sett](const ThreadInfo &ti) {
      try {
        TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);

        logger_info("engine", _settings->alias, ": compressing ", fname);
        auto start_time = clock();
        auto storage_path = sett->raw_path.value();
        auto full_path = fs::append_path(storage_path, fname);

        WALFile_Ptr wal=WALFile::open(env, full_path, true);

        auto all = wal->readAll();

        this->write_wal_to_page(fname, all);

        auto end = clock();
        auto elapsed = double(end - start_time) / CLOCKS_PER_SEC;

        logger_info("engine", _settings->alias, ": compressing ", fname, " done. elapsed time - ", elapsed);
      } catch (std::exception &ex) {
        THROW_EXCEPTION("Dropper::drop_wal_internal: ", ex.what());
      }
      return false;
    };

    auto handle = ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
    handle->wait();
    this->_dropper_lock.unlock();
  }
  _is_stoped = true;
}

void Dropper::write_wal_to_page(const std::string &fname, std::shared_ptr<MeasArray> ma) {
  auto pm = _page_manager.get();
  auto am = _wal_manager.get();
  auto sett = _settings;

  auto storage_path = sett->raw_path.value();
  auto full_path = fs::append_path(storage_path, fname);

  std::sort(ma->begin(), ma->end(), meas_time_compare_less());

  auto without_path = fs::extract_filename(fname);
  auto page_fname = fs::filename(without_path);

  pm->append(page_fname, *ma.get());
  am->erase(fname);
}

void Dropper::flush() {
  logger_info("engine", _settings->alias, ": Dropper flush...");
  while (this->description().wal != 0) {
    this->_cond_var.notify_all();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  logger_info("engine", _settings->alias, ": Dropper flush end.");
}
