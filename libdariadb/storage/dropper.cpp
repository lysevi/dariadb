#include <libdariadb/storage/dropper.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/storage/pages/page.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/utils/metrics.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <ctime>

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

void Dropper::dropAof(const std::string& fname) {
  std::lock_guard<std::mutex> lg(_queue_locker);
  auto fres = _files_queue.find(fname);
  if (fres != _files_queue.end()) {
    return;
  }
  auto storage_path = _settings->raw_path.value();
  if (utils::fs::path_exists(utils::fs::append_path(storage_path, fname))) {
	  _files_queue.emplace(fname);
    _in_queue++;
    drop_aof_internal(fname);
  }
}

void Dropper::cleanStorage(const std::string&storagePath) {
	logger_info("engine: dropper - check storage.");
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
      TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);
	  if(!this->_dropper_lock.try_lock()) {
		  return true;
	  }
      TIMECODE_METRICS(ctmd, "drop", "Dropper::drop_aof_internal");
	  
	  logger_info("engine: compressing ", fname);
      auto start_time = clock();
      auto storage_path = sett->raw_path.value();
      auto full_path = fs::append_path(storage_path, fname);

      AOFile_Ptr aof{new AOFile(env, full_path, true)};

      auto all = aof->readAll();

      this->write_aof_to_page(fname, all);

	  this->_queue_locker.lock();
      this->_in_queue--;
      this->_files_queue.erase(fname);
      this->_queue_locker.unlock();
      auto end = clock();
      auto elapsed = double(end - start_time) / CLOCKS_PER_SEC;
	  this->_dropper_lock.unlock();
	  logger_info("engine: compressing ", fname, " done. elapsed time - ", elapsed);
    } catch (std::exception &ex) {
      THROW_EXCEPTION("Dropper::drop_aof_internal: ", ex.what());
    }
	return false;
  };
  ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
}

void Dropper::write_aof_to_page(const std::string &fname, std::shared_ptr<MeasArray> ma) {
  auto pm = _page_manager.get();
  auto am = _aof_manager.get();
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
  logger_info("engine: Dropper flush...");
  while (_in_queue != 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  logger_info("engine: Dropper flush end.");
}
