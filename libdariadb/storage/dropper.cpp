#include <libdariadb/storage/dropper.h>
#include <libdariadb/storage/lock_manager.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/storage/page.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/utils/metrics.h>
#include <libdariadb/utils/thread_manager.h>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils;
using namespace dariadb::utils::async;

Dropper::Dropper(EngineEnvironment_ptr engine_env, PageManager_ptr page_manager,
                 AOFManager_ptr aof_manager)
    : _in_queue(0), _page_manager(page_manager), _aof_manager(aof_manager),
      _engine_env(engine_env) {
  _settings =
      _engine_env->getResourceObject<Settings>(EngineEnvironment::Resource::SETTINGS);
}

Dropper::~Dropper() {}

Dropper::Description Dropper::description() const {
  Dropper::Description result;
  result.aof = _in_queue.load();
  return result;
}

void Dropper::drop_aof(const std::string& fname) {
  std::lock_guard<std::mutex> lg(_locker);
  auto fres = _addeded_files.find(fname);
  if (fres != _addeded_files.end()) {
    return;
  }
  auto storage_path = _settings->path;
  if (utils::fs::path_exists(utils::fs::append_path(storage_path, fname))) {
    _addeded_files.emplace(fname);
    _in_queue++;
    drop_aof_internal(fname);
  }
}

void Dropper::cleanStorage(const std::string&storagePath) {
  auto aofs_lst = fs::ls(storagePath, AOF_FILE_EXT);
  auto page_lst = fs::ls(storagePath, PAGE_FILE_EXT);

  for (auto &aof : aofs_lst) {
    auto aof_fname = fs::filename(aof);
    for (auto &pagef : page_lst) {
      auto page_fname = fs::filename(pagef);
      if (page_fname == aof_fname) {
        logger_info("engine: fsck aof drop not finished: ", aof_fname);
        logger_info("engine: fsck rm ", pagef);
        PageManager::erase(storagePath, fs::extract_filename(pagef));
      }
    }
  }
}

void Dropper::drop_aof_internal(const std::string &fname) {
  auto env = _engine_env;
  auto sett = _settings;
  AsyncTask at = [fname, this, env, sett](const ThreadInfo &ti) {
    try {
      TKIND_CHECK(THREAD_COMMON_KINDS::DISK_IO, ti.kind);
      TIMECODE_METRICS(ctmd, "drop", "Dropper::drop_aof_internal");

      auto storage_path = sett->path;
      auto full_path = fs::append_path(storage_path, fname);

      AOFile_Ptr aof{new AOFile(env, full_path, true)};

      auto all = aof->readAll();

      this->write_aof_to_page(fname, all);

	  this->_locker.lock();
	  this->_in_queue--;
	  this->_addeded_files.erase(fname);
	  this->_locker.unlock();
    } catch (std::exception &ex) {
      THROW_EXCEPTION("Dropper::drop_aof_internal: ", ex.what());
    }
  };
  ThreadManager::instance()->post(THREAD_COMMON_KINDS::DISK_IO, AT(at));
}

void Dropper::write_aof_to_page(const std::string &fname, std::shared_ptr<MeasArray> ma) {
  auto pm = _page_manager.get();
  auto am = _aof_manager.get();
  auto lm = _engine_env->getResourceObject<LockManager>(
      EngineEnvironment::Resource::LOCK_MANAGER);
  auto sett = _settings;

  auto storage_path = sett->path;
  auto full_path = fs::append_path(storage_path, fname);

  std::sort(ma->begin(), ma->end(), meas_time_compare_less());

  auto without_path = fs::extract_filename(fname);
  auto page_fname = fs::filename(without_path);

  lm->lock(LOCK_KIND::EXCLUSIVE, {LOCK_OBJECTS::DROP_AOF});
  pm->append(page_fname, *ma.get());
  am->erase(fname);
  lm->unlock(LOCK_OBJECTS::DROP_AOF);
}

void Dropper::flush() {
  logger_info("engine: Dropper flush...");
  while (_in_queue != 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  logger_info("engine: Dropper flush end.");
}
