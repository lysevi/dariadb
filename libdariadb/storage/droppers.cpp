#include "droppers.h"
#include "../utils/metrics.h"
#include "../utils/thread_manager.h"
#include "aof_manager.h"
#include "capacitor_manager.h"
#include "lock_manager.h"
#include "manifest.h"
#include "options.h"
#include "page.h"
#include "page_manager.h"

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils;
using namespace dariadb::utils::async;

void AofDropper::drop(const std::string &fname, const std::string &storage_path) {

  auto target_name = fs::filename(fname) + CAP_FILE_EXT;
  if (fs::path_exists(fs::append_path(storage_path, target_name))) {
    return;
  }

  auto full_path = fs::append_path(storage_path, fname);

  AOFile_Ptr aof{new AOFile(full_path, true)};

  auto ma = aof->readAll();

  std::sort(ma.begin(), ma.end(), meas_time_compare_less());

  CapacitorManager::instance()->append(target_name, ma);

  aof = nullptr;
  AOFManager::instance()->erase(fname);
}

void AofDropper::drop(const std::string fname) {
  AsyncTask at = [fname, this](const ThreadInfo &ti) {
    try {
      TKIND_CHECK(THREAD_COMMON_KINDS::DROP, ti.kind);
      TIMECODE_METRICS(ctmd, "drop", "AofDropper::drop");
      LockManager::instance()->lock(LockKind::EXCLUSIVE, LockObjects::DROP_AOF);

      AofDropper::drop(fname, Options::instance()->path);

      LockManager::instance()->unlock(LockObjects::DROP_AOF);
    } catch (std::exception &ex) {
      THROW_EXCEPTION_SS("AofDropper::drop: " << ex.what());
    }
  };

  ThreadManager::instance()->post(THREAD_COMMON_KINDS::DROP, AT(at));
}

// on start, rm COLA files with name exists AOF file.
void AofDropper::cleanStorage(std::string storagePath) {
  auto aofs_lst = fs::ls(storagePath, AOF_FILE_EXT);
  auto caps_lst = fs::ls(storagePath, CAP_FILE_EXT);

  for (auto &aof : aofs_lst) {
    auto aof_fname = fs::filename(aof);
    for (auto &capf : caps_lst) {
      auto cap_fname = fs::filename(capf);
      if (cap_fname == aof_fname) {
        logger_info("fsck: aof drop not finished: " << aof_fname);
        logger_info("fsck: rm " << capf);
        CapacitorManager::instance()->erase(fs::extract_filename(capf));
      }
    }
  }
}

void CapDrooper::drop(const std::string &fname) {
  AsyncTask at = [fname, this](const ThreadInfo &ti) {
    try {
      TKIND_CHECK(THREAD_COMMON_KINDS::DROP, ti.kind);
      TIMECODE_METRICS(ctmd, "drop", "CapDrooper::drop");

      LockManager::instance()->lock(LockKind::EXCLUSIVE, LockObjects::DROP_CAP);
      auto cap = Capacitor_Ptr{new Capacitor{fname, false}};

      auto without_path = fs::extract_filename(fname);
      auto page_fname = fs::filename(without_path);
      auto all = cap->readAll();
      assert(all.size() == cap->size());
      PageManager::instance()->append(page_fname, all);

      cap = nullptr;
      CapacitorManager::instance()->erase(without_path);
      LockManager::instance()->unlock(LockObjects::DROP_CAP);
    } catch (std::exception &ex) {
      THROW_EXCEPTION_SS("CapDrooper::drop: " << ex.what());
    }
  };
  ThreadManager::instance()->post(THREAD_COMMON_KINDS::DROP, AT(at));
}

// on start, rm PAGE files with name exists CAP file.
void CapDrooper::cleanStorage(std::string storagePath) {
  auto caps_lst = fs::ls(storagePath, CAP_FILE_EXT);
  auto page_lst = fs::ls(storagePath, dariadb::storage::PAGE_FILE_EXT);
  for (auto &cap : caps_lst) {
    auto cap_fname = fs::filename(cap);
    for (auto &page : page_lst) {
      auto page_fname = fs::filename(page);
      if (cap_fname == page_fname) {
        logger_info("fsck: cap drop not finished: " << page_fname);
        logger_info("fsck: rm " << page_fname);
        PageManager::instance()->erase(fs::extract_filename(page));
        /*fs::rm(page_fname);
        fs::rm(page_fname + "i");
        Manifest::instance()->page_rm(fs::extract_filename(page));*/
      }
    }
  }
}
