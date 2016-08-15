#include "dropper.h"
#include "../utils/metrics.h"
#include "../utils/thread_manager.h"
#include "aof_manager.h"
#include "capacitor_manager.h"
#include "lock_manager.h"
#include "manifest.h"
#include "options.h"
#include "page.h"
#include "page_manager.h"
#include "options.h"


using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils;
using namespace dariadb::utils::async;

const std::chrono::milliseconds DEFALT_DROP_PERIOD(500);

Dropper::Dropper()
    : PeriodWorker(std::chrono::milliseconds(DEFALT_DROP_PERIOD)) {
  this->period_worker_start();
}

Dropper::~Dropper() { this->period_worker_stop(); }

Dropper::Queues Dropper::queues() const {
  Dropper::Queues result;
  result.aof = _aof_files.size();
  result.cap = _cap_files.size();
  return result;
}

void Dropper::drop_aof(const std::string &fname,
                       const std::string &storage_path) {

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

void Dropper::drop_aof(const std::string fname) {
  std::lock_guard<utils::Locker> lg(_locker);
  _aof_files.push_back(fname);
}

void Dropper::drop_cap(const std::string &fname) {
  std::lock_guard<utils::Locker> lg(_locker);
  _cap_files.push_back(fname);
}

void Dropper::cleanStorage(std::string storagePath) {
  auto aofs_lst = fs::ls(storagePath, AOF_FILE_EXT);
  auto caps_lst = fs::ls(storagePath, CAP_FILE_EXT);

  for (auto &aof : aofs_lst) {
    auto aof_fname = fs::filename(aof);
    for (auto &capf : caps_lst) {
      auto cap_fname = fs::filename(capf);
      if (cap_fname == aof_fname) {
        logger_info("fsck: aof drop not finished: " << aof_fname);
        logger_info("fsck: rm " << capf);
        CapacitorManager::erase(fs::extract_filename(capf));
      }
    }
  }

  caps_lst = fs::ls(storagePath, CAP_FILE_EXT);
  auto page_lst = fs::ls(storagePath, dariadb::storage::PAGE_FILE_EXT);
  for (auto &cap : caps_lst) {
    auto cap_fname = fs::filename(cap);
    for (auto &page : page_lst) {
      auto page_fname = fs::filename(page);
      if (cap_fname == page_fname) {
        logger_info("fsck: cap drop not finished: " << page_fname);
        logger_info("fsck: rm " << page_fname);
        PageManager::erase(fs::extract_filename(page));
        /*fs::rm(page_fname);
        fs::rm(page_fname + "i");
        Manifest::instance()->page_rm(fs::extract_filename(page));*/
      }
    }
  }
}

void Dropper::drop_aof_internal(const std::string fname) {
  AsyncTask at = [fname, this](const ThreadInfo &ti) {
    try {
      TKIND_CHECK(THREAD_COMMON_KINDS::DROP, ti.kind);
      TIMECODE_METRICS(ctmd, "drop", "Dropper::drop_aof_internal");
      LockManager::instance()->lock(LockKind::EXCLUSIVE, LockObjects::DROP_AOF);

      Dropper::drop_aof(fname, Options::instance()->path);

      LockManager::instance()->unlock(LockObjects::DROP_AOF);
    } catch (std::exception &ex) {
      THROW_EXCEPTION_SS("Dropper::drop_aof_internal: " << ex.what());
    }
  };

  auto res = ThreadManager::instance()->post(THREAD_COMMON_KINDS::DROP, AT(at));
  res->wait();
}

void Dropper::drop_cap_internal(const std::string &fname) {
  AsyncTask at = [fname, this](const ThreadInfo &ti) {
    try {
      TKIND_CHECK(THREAD_COMMON_KINDS::DROP, ti.kind);
      TIMECODE_METRICS(ctmd, "drop", "Dropper::drop_cap_internal");

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
      THROW_EXCEPTION_SS("Dropper::drop_cap_internal: " << ex.what());
    }
  };
  auto res = ThreadManager::instance()->post(THREAD_COMMON_KINDS::DROP, AT(at));
  res->wait();
}

void Dropper::drop_aof_to_compress(const std::string &fname){
    AsyncTask at = [fname, this](const ThreadInfo &ti) {
      try {
        TKIND_CHECK(THREAD_COMMON_KINDS::DROP, ti.kind);
        TIMECODE_METRICS(ctmd, "drop", "Dropper::drop_aof_to_compress");

        auto storage_path=Options::instance()->path;
        LockManager::instance()->lock(LockKind::EXCLUSIVE, {LockObjects::AOF, LockObjects::PAGE});

        auto full_path = fs::append_path(storage_path, fname);

        AOFile_Ptr aof{new AOFile(full_path, true)};

        auto all = aof->readAll();

        std::sort(all.begin(), all.end(), meas_time_compare_less());


        auto without_path = fs::extract_filename(fname);
        auto page_fname = fs::filename(without_path);
        PageManager::instance()->append(page_fname, all);


        aof = nullptr;
        AOFManager::instance()->erase(fname);

        LockManager::instance()->unlock({LockObjects::AOF, LockObjects::PAGE});
      } catch (std::exception &ex) {
        THROW_EXCEPTION_SS("Dropper::drop_aof_to_compress: " << ex.what());
      }
    };
    auto res = ThreadManager::instance()->post(THREAD_COMMON_KINDS::DROP, AT(at));
    res->wait();
}

void Dropper::flush() {
    logger_info("Dropper: wait period end...");
  std::lock_guard<std::mutex> lg(_period_locker);
  size_t iter = 0;
  auto strat=Options::instance()->strategy;
  while (!_aof_files.empty() || !_cap_files.empty()) {
    logger_info("flush iter=" << iter++);
    _locker.lock();
    auto aof_copy = _aof_files;
    auto cap_copy = _cap_files;

    _aof_files.clear();
    _cap_files.clear();
    _locker.unlock();

    logger("aof to flush:" << aof_copy.size());
    for (auto f : aof_copy) {
        switch (strat) {
        case STRATEGY::COMPRESSED:
            drop_aof_to_compress(f);
            break;
        default:
            drop_aof_internal(f);
            break;
        }
    }

    logger("cap to flush:" << cap_copy.size());
    for (auto f : cap_copy) {
      drop_cap_internal(f);
    }
  }
}

void Dropper::period_call() {
  std::lock_guard<std::mutex> lg(_period_locker);

  auto strat=Options::instance()->strategy;

  if (!_aof_files.empty()) {
    _locker.lock();
    auto copy = _aof_files;
    _locker.unlock();
    for (auto f : copy) {
        switch (strat) {
        case STRATEGY::COMPRESSED:
            drop_aof_to_compress(f);
            break;
        default:
            drop_aof_internal(f);
            break;
        }


      _locker.lock();
      _aof_files.remove(f);
      _locker.unlock();
    }
  }

  if (!_cap_files.empty()) {
    _locker.lock();

    for (auto f : _cap_files) {
      drop_cap_internal(f);
    }
    _cap_files.clear();
    _locker.unlock();
  }
}
