#include "lock_manager.h"
#include "../utils/logger.h"
#include <sstream>
using namespace dariadb;
using namespace dariadb::storage;

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

RWMutex_Ptr LockManager::get_or_create_lock_object(const LockObjects &lo) {
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

RWMutex_Ptr LockManager::get_lock_object(const LockObjects &lo) {
  std::lock_guard<std::mutex> lg(_mutex);
  auto lock_target_it = _lockers.find(lo);
  if (lock_target_it != _lockers.end()) {
    return lock_target_it->second;
  } else {
    throw MAKE_EXCEPTION("unlock unknow object.");
  }
}

void LockManager::lock(const LockKind &lk, const LockObjects &lo) {

  switch (lo) {
  case LockObjects::AOF:
  case LockObjects::CAP:
  case LockObjects::PAGE:
    lock_by_kind(lk, lo);
    break;
  case LockObjects::DROP_AOF: {
    lock_drop_aof();
    break;
  }
  case LockObjects::DROP_CAP: {
    lock_drop_cap();
    break;
  }
  default: {
    std::stringstream ss;
    ss << "Unknow LockObject:" << (uint8_t)lo;
    throw MAKE_EXCEPTION(ss.str());
    break;
  }
  }
}

void LockManager::lock(const LockKind &lk, const std::vector<LockObjects> &los) {
  std::vector<RWMutex_Ptr> rw_mtx{los.size()};
  for (size_t i = 0; i < los.size(); ++i) {
    if ((los[i] == LockObjects::DROP_AOF) || (los[i] == LockObjects::DROP_CAP)) {
      throw MAKE_EXCEPTION("Only simple locks support");
    }
    rw_mtx[i] = get_or_create_lock_object(los[i]);
  }
  bool success = false;
  while (!success) {
    bool local_status = false;
    for (size_t i = 0; i < los.size(); ++i) {
      switch (lk) {
      case LockKind::EXCLUSIVE: {
        local_status = rw_mtx[i]->mutex.try_lock_upgrade();
        break;
      }
      case LockKind::READ: {
        local_status = rw_mtx[i]->mutex.try_lock_shared();
        break;
      }
      };
      if (!local_status) {
        for (size_t j = 0; j < i; ++j) {
          switch (lk) {
          case LockKind::EXCLUSIVE: {
            rw_mtx[j]->mutex.unlock_and_lock_upgrade();
            break;
          }
          case LockKind::READ: {
            rw_mtx[i]->mutex.unlock_shared();
            break;
          }
          };
        }
        break;
      } else {
        success = true;
      }
    }
  }
}

void LockManager::unlock(const LockObjects &lo) {
  switch (lo) {
  case LockObjects::AOF:
  case LockObjects::CAP:
  case LockObjects::PAGE: {
    auto lock_target = get_lock_object(lo);
    switch (lock_target->kind) {
    case LockKind::EXCLUSIVE:
      lock_target->mutex.unlock_upgrade();
      break;
    case LockKind::READ:
      lock_target->mutex.unlock_shared();
      break;
    };
    break;
  }
  case LockObjects::DROP_AOF: {
    auto aof_locker = get_lock_object(LockObjects::AOF);
    auto cap_locker = get_lock_object(LockObjects::CAP);
    aof_locker->mutex.unlock_upgrade();
    cap_locker->mutex.unlock_upgrade();
    break;
  }
  case LockObjects::DROP_CAP: {
    auto page_locker = get_lock_object(LockObjects::PAGE);
    auto cap_locker = get_lock_object(LockObjects::CAP);
    page_locker->mutex.unlock_upgrade();
    cap_locker->mutex.unlock_upgrade();
    break;
  }
  }
}

void LockManager::lock_by_kind(const LockKind &lk, const LockObjects &lo) {
  auto lock_target = get_or_create_lock_object(lo);
  switch (lk) {
  case LockKind::EXCLUSIVE:
    lock_target->mutex.lock_upgrade();
    break;
  case LockKind::READ:
    lock_target->mutex.lock_shared();
    break;
  };
  lock_target->kind = lk;
}

void LockManager::lock_drop_aof() {
  bool success = false;
  auto aof_locker = get_or_create_lock_object(LockObjects::AOF);
  auto cap_locker = get_or_create_lock_object(LockObjects::CAP);
  while (!success) {
    if (!aof_locker->mutex.try_lock_upgrade()) {
      continue;
    }

    if (!cap_locker->mutex.try_lock_upgrade()) {
      aof_locker->mutex.unlock_and_lock_upgrade();
      continue;
    }

    aof_locker->kind = LockKind::EXCLUSIVE;
    cap_locker->kind = LockKind::EXCLUSIVE;
    success = true;
  }
}

void LockManager::lock_drop_cap() {
  bool success = false;
  auto page_locker = get_or_create_lock_object(LockObjects::PAGE);
  auto cap_locker = get_or_create_lock_object(LockObjects::CAP);
  while (!success) {
    if (!page_locker->mutex.try_lock_upgrade()) {
      continue;
    }

    if (!cap_locker->mutex.try_lock_upgrade()) {
      page_locker->mutex.unlock_and_lock_upgrade();
      continue;
    }

    page_locker->kind = LockKind::EXCLUSIVE;
    cap_locker->kind = LockKind::EXCLUSIVE;
    success = true;
  }
}