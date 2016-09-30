#include "dropper.h"
#include "../utils/metrics.h"
#include "../utils/thread_manager.h"
#include "aof_manager.h"
#include "lock_manager.h"
#include "manifest.h"
#include "options.h"
#include "options.h"
#include "page.h"
#include "page_manager.h"

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils;
using namespace dariadb::utils::async;

Dropper::Dropper() : _in_queue(0) {}

Dropper::~Dropper() {}

Dropper::Queues Dropper::queues() const {
  Dropper::Queues result;
  result.aof = _in_queue.load();
  return result;
}

void Dropper::drop_aof(const std::string &fname, const std::string &storage_path) {
  LockManager::instance()->lock(LOCK_KIND::EXCLUSIVE,
                                {LOCK_OBJECTS::AOF, LOCK_OBJECTS::PAGE});

  auto full_path = fs::append_path(storage_path, fname);

  AOFile_Ptr aof{new AOFile(full_path, true)};

  auto all = aof->readAll();

  std::sort(all->begin(), all->end(), meas_time_compare_less());

  auto without_path = fs::extract_filename(fname);
  auto page_fname = fs::filename(without_path);
  PageManager::instance()->append(page_fname, *all.get());

  aof = nullptr;
  AOFManager::instance()->erase(fname);

  LockManager::instance()->unlock({LOCK_OBJECTS::AOF, LOCK_OBJECTS::PAGE});
}

void Dropper::drop_aof(const std::string fname) {
	std::lock_guard<std::mutex> lg(_locker);
	auto fres=_addeded_files.find(fname);
	if (fres != _addeded_files.end()) {
		return;
	}
	auto storage_path = Options::instance()->path;
	if (utils::fs::path_exists(utils::fs::append_path(storage_path, fname))) {
		_addeded_files.insert(fname);
		_in_queue++;
		drop_aof_internal(fname);
	}
}

void Dropper::cleanStorage(std::string storagePath) {
  auto aofs_lst = fs::ls(storagePath, AOF_FILE_EXT);
  auto page_lst = fs::ls(storagePath, PAGE_FILE_EXT);

  for (auto &aof : aofs_lst) {
    auto aof_fname = fs::filename(aof);
    for (auto &pagef : page_lst) {
      auto page_fname = fs::filename(pagef);
      if (page_fname == aof_fname) {
        logger_info("engine: fsck aof drop not finished: ", aof_fname);
        logger_info("engine: fsck rm ", pagef);
        PageManager::erase(fs::extract_filename(pagef));
      }
    }
  }
}

void Dropper::drop_aof_internal(const std::string fname) {
  AsyncTask at = [fname, this](const ThreadInfo &ti) {
    try {
      TKIND_CHECK(THREAD_COMMON_KINDS::DISK_IO, ti.kind);
      TIMECODE_METRICS(ctmd, "drop", "Dropper::drop_aof_internal");

      auto storage_path = Options::instance()->path;
      auto full_path = fs::append_path(storage_path, fname);

	  AOFile_Ptr aof{new AOFile(full_path, true)};

	  auto all = aof->readAll();
	  
	  this->write_aof_to_page(fname, all);
    } catch (std::exception &ex) {
      THROW_EXCEPTION("Dropper::drop_aof_internal: ", ex.what());
    }
  };
  ThreadManager::instance()->post(THREAD_COMMON_KINDS::DISK_IO, AT(at));
}

void Dropper::write_aof_to_page(const std::string fname, std::shared_ptr<MeasArray> ma) {
	AsyncTask at = [fname, this, ma](const ThreadInfo &ti) {
		try {
			TKIND_CHECK(THREAD_COMMON_KINDS::DROP, ti.kind);
			TIMECODE_METRICS(ctmd, "drop", "Dropper::write_aof_to_page");
		
			auto storage_path = Options::instance()->path;
			auto full_path = fs::append_path(storage_path, fname);

			std::sort(ma->begin(), ma->end(), meas_time_compare_less());

			auto without_path = fs::extract_filename(fname);
			auto page_fname = fs::filename(without_path);

			LockManager::instance()->lock(LOCK_KIND::EXCLUSIVE, { LOCK_OBJECTS::DROP_AOF });
			PageManager::instance()->append(page_fname, *ma.get());
			AOFManager::instance()->erase(fname);
			LockManager::instance()->unlock(LOCK_OBJECTS::DROP_AOF);
		}
		catch (std::exception &ex) {
			THROW_EXCEPTION("Dropper::write_aof_to_page: ", ex.what());
		}
		this->_locker.lock();
		this->_in_queue--;
		this->_addeded_files.erase(fname);
		this->_locker.unlock();
	};
	ThreadManager::instance()->post(THREAD_COMMON_KINDS::DROP, AT(at));
}

void Dropper::flush() {
  logger_info("engine: Dropper flush...");
  while (_in_queue != 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  logger_info("engine: Dropper flush end.");
}