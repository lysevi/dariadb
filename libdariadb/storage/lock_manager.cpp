#include "lock_manager.h"
#include "../utils/exception.h"
#include "../utils/logger.h"
#include <sstream>
using namespace dariadb;
using namespace dariadb::storage;

void lock_mutex(LOCK_KIND kind, const RWMutex_Ptr &mtx) {
  switch (kind) {
  case LOCK_KIND::EXCLUSIVE: {
    mtx->mutex.lock();
    break;
  }
  case LOCK_KIND::READ: {
    mtx->mutex.lock_shared();
    break;
  }
  case LOCK_KIND::UNKNOW: {
    THROW_EXCEPTION("try to lock unknow state");
  }
  };
}

bool try_lock_mutex(LOCK_KIND kind, const RWMutex_Ptr &mtx) {
  switch (kind) {
  case LOCK_KIND::EXCLUSIVE: {
    return mtx->mutex.try_lock();
  }
  case LOCK_KIND::READ: {
    return mtx->mutex.try_lock_shared();
  }
  case LOCK_KIND::UNKNOW: {
    THROW_EXCEPTION("try to try-lock unknow state");
  }
  };
  return false;
}

void unlock_mutex(const RWMutex_Ptr &mtx) {
  switch (mtx->kind) {
  case LOCK_KIND::EXCLUSIVE: {
    mtx->mutex.unlock();
    break;
  }
  case LOCK_KIND::READ: {
    mtx->mutex.unlock_shared();
    break;
  }
  case LOCK_KIND::UNKNOW: {
    THROW_EXCEPTION("try to unlock unknow state");
  }
  };
}

LockManager *LockManager::_instance = nullptr;

LockManager::~LockManager() {}

LockManager::LockManager(const LockManager::Params &param) {}

void LockManager::start(const LockManager::Params &param) {
  if (LockManager::_instance == nullptr) {
    LockManager::_instance = new LockManager(param);
  } else {
    throw MAKE_EXCEPTION("LockManager::start started twice.");
  }
}

void LockManager::stop() {
  delete LockManager::_instance;
  LockManager::_instance = nullptr;
}

LockManager *LockManager::instance() {
  return _instance;
}

RWMutex_Ptr LockManager::get_or_create_lock_object(const LOCK_OBJECTS &lo) {
  std::lock_guard<std::mutex> lg(_mutex);
  auto lock_target_it = _lockers.find(lo);
  if (lock_target_it != _lockers.end()) {
    return lock_target_it->second;
  } else {
    RWMutex_Ptr lock_target{new MutexWrap{}};
    _lockers.insert(std::make_pair(lo, lock_target));
    return lock_target;
  }
}

RWMutex_Ptr LockManager::get_lock_object(const LOCK_OBJECTS &lo) {
  std::lock_guard<std::mutex> lg(_mutex);
  auto lock_target_it = _lockers.find(lo);
  if (lock_target_it != _lockers.end()) {
    return lock_target_it->second;
  } else {
    throw MAKE_EXCEPTION("unlock unknow object.");
  }
}

void LockManager::lock(const LOCK_KIND &lk, const LOCK_OBJECTS &lo) {

  switch (lo) {
  case LOCK_OBJECTS::AOF:
  case LOCK_OBJECTS::CAP:
  case LOCK_OBJECTS::PAGE:
    lock_by_kind(lk, lo);
    break;
  case LOCK_OBJECTS::DROP_AOF: {
    lock_drop_aof();
    break;
  }
  case LOCK_OBJECTS::DROP_CAP: {
    lock_drop_cap();
    break;
  }
  default: {
    THROW_EXCEPTION("Unknow LockObject:", (uint8_t)lo);
  }
  }
}

void LockManager::lock(const LOCK_KIND &lk, const std::vector<LOCK_OBJECTS> &los) {
  std::vector<RWMutex_Ptr> rw_mtx{los.size()};
  for (size_t i = 0; i < los.size(); ++i) {
    if ((los[i] == LOCK_OBJECTS::DROP_AOF) || (los[i] == LOCK_OBJECTS::DROP_CAP)) {
      throw MAKE_EXCEPTION("Only simple locks support");
    }
    rw_mtx[i] = get_or_create_lock_object(los[i]);
  }
  bool success = false;
  while (!success) {
    bool local_status = false;
    for (size_t i = 0; i < los.size(); ++i) {
      local_status = try_lock_mutex(lk, rw_mtx[i]);

      if (!local_status) {
        for (size_t j = 0; j < i; ++j) {
          unlock_mutex(rw_mtx[j]);
        }
        success = false;
        break;
      } else {
        rw_mtx[i]->kind = lk;
        success = true;
      }
    }
  }
}

void LockManager::unlock(const LOCK_OBJECTS &lo) {
  switch (lo) {
  case LOCK_OBJECTS::AOF:
  case LOCK_OBJECTS::CAP:
  case LOCK_OBJECTS::PAGE: {
    auto lock_target = get_lock_object(lo);
    unlock_mutex(lock_target);
    break;
  }
  case LOCK_OBJECTS::DROP_AOF: {
    auto aof_locker = get_lock_object(LOCK_OBJECTS::AOF);
    auto cap_locker = get_lock_object(LOCK_OBJECTS::CAP);
    aof_locker->mutex.unlock();
    cap_locker->mutex.unlock();
    break;
  }
  case LOCK_OBJECTS::DROP_CAP: {
    auto page_locker = get_lock_object(LOCK_OBJECTS::PAGE);
    auto cap_locker = get_lock_object(LOCK_OBJECTS::CAP);
    page_locker->mutex.unlock();
    cap_locker->mutex.unlock();
    break;
  }
  }
}

void LockManager::unlock(const std::vector<LOCK_OBJECTS> &los) {
  for (auto lo : los) {
    this->unlock(lo);
  }
}

void LockManager::lock_by_kind(const LOCK_KIND &lk, const LOCK_OBJECTS &lo) {
  auto lock_target = get_or_create_lock_object(lo);
  lock_mutex(lk, lock_target);
}

void LockManager::lock_drop_aof() {
  lock(LOCK_KIND::EXCLUSIVE, {LOCK_OBJECTS::AOF, LOCK_OBJECTS::CAP});
}

void LockManager::lock_drop_cap() {
  lock(LOCK_KIND::EXCLUSIVE, {LOCK_OBJECTS::PAGE, LOCK_OBJECTS::CAP});
}
