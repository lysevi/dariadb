#include "droppers.h"
#include "manifest.h"
#include "lock_manager.h"
#include "options.h"
#include "page_manager.h"
#include "page.h"
#include "../utils/thread_manager.h"
#include "../utils/metrics.h"

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils::async;

void AofDropper::drop(const AOFile_Ptr aof, const std::string &fname,
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

void AofDropper::drop(const std::string fname,
                      const uint64_t aof_size) {
  AsyncTask at = [fname, aof_size, this](const ThreadInfo &ti) {
    TKIND_CHECK(THREAD_COMMON_KINDS::DROP, ti.kind);
    TIMECODE_METRICS(ctmd, "drop", "AofDropper::drop");
    LockManager::instance()->lock(LockKind::EXCLUSIVE, LockObjects::DROP_AOF);

    auto full_path =
        dariadb::utils::fs::append_path(Options::instance()->path, fname);

    AOFile_Ptr aof{new AOFile(full_path, true)};
    AofDropper::drop(aof, fname, Options::instance()->path);
    aof = nullptr;
    LockManager::instance()->unlock(LockObjects::DROP_AOF);
  };

  ThreadManager::instance()->post(THREAD_COMMON_KINDS::DROP, AT(at));
}

void AofDropper::cleanStorage(std::string storagePath) {
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

void CapDrooper::drop(const std::string &fname){
  AsyncTask at = [fname, this](const ThreadInfo &ti) {
    TKIND_CHECK(THREAD_COMMON_KINDS::DROP, ti.kind);
    TIMECODE_METRICS(ctmd, "drop", "CapDrooper::drop");

    LockManager::instance()->lock(LockKind::EXCLUSIVE, LockObjects::DROP_CAP);
    auto cap = Capacitor_Ptr{new Capacitor{fname, false}};

    auto without_path = utils::fs::extract_filename(fname);
    auto page_fname = utils::fs::filename(without_path);
    auto all = cap->readAll();
    assert(all.size() == cap->size());
    PageManager::instance()->append(page_fname, all);
    Manifest::instance()->cola_rm(without_path);
    cap = nullptr;
    utils::fs::rm(fname);

    LockManager::instance()->unlock(LockObjects::DROP_CAP);
  };
  ThreadManager::instance()->post(THREAD_COMMON_KINDS::DROP, AT(at));
}

// on start, rm PAGE files with name exists CAP file.
void CapDrooper::cleanStorage(std::string storagePath) {
  auto caps_lst = utils::fs::ls(storagePath, CAP_FILE_EXT);
  auto page_lst = utils::fs::ls(storagePath, dariadb::storage::PAGE_FILE_EXT);
  for (auto &cap : caps_lst) {
    auto cap_fname = utils::fs::filename(cap);
    for (auto &page : page_lst) {
      auto page_fname = utils::fs::filename(page);
      if (cap_fname == page_fname) {
        logger_info("fsck: cap drop not finished: " << page_fname);
        logger_info("fsck: rm " << page_fname);
        utils::fs::rm(page_fname);
        utils::fs::rm(page_fname + "i");
        Manifest::instance()->page_rm(utils::fs::extract_filename(page));
      }
    }
  }
}
