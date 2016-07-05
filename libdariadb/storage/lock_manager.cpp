#include "lock_manager.h"
#include "../utils/logger.h"

using namespace dariadb;
using namespace dariadb::storage;

LockManager *LockManager::_instance = nullptr;

dariadb::storage::LockManager::~LockManager() {}

dariadb::storage::LockManager::LockManager(const LockManager::Params &param) {}

void dariadb::storage::LockManager::start(const LockManager::Params &param) {
  if (LockManager::_instance == nullptr) {
    LockManager::_instance = new LockManager(param);
  } else {
    throw MAKE_EXCEPTION("LockManager::start started twice.");
  }
}

void dariadb::storage::LockManager::stop() {
  delete LockManager::_instance;
  LockManager::_instance = nullptr;
}

LockManager *dariadb::storage::LockManager::instance() {
  return _instance;
}

RWMutex_Ptr
dariadb::storage::LockManager::get_or_create_lock_object(const LockObjects &lo) {
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

RWMutex_Ptr dariadb::storage::LockManager::get_lock_object(const LockObjects &lo) {
  std::lock_guard<std::mutex> lg(_mutex);
  auto lock_target_it = _lockers.find(lo);
  if (lock_target_it != _lockers.end()) {
    return lock_target_it->second;
  } else {
    throw MAKE_EXCEPTION("unlock unknow object.");
  }
}

void dariadb::storage::LockManager::lock_by_kind(const LockKind &lk,
                                                 const LockObjects &lo) {
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

void dariadb::storage::LockManager::lock(const LockKind &lk, const LockObjects &lo) {

  switch (lo) {
  case LockObjects::AOF:
  case LockObjects::CAP:
  case LockObjects::PAGE:
    lock_by_kind(lk, lo);
    break;
  case LockObjects::DROP_AOF:
    break;
  case LockObjects::DROP_CAP:
    break;
  default:
    break;
  }
}

void dariadb::storage::LockManager::unlock(const LockObjects &lo) {
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
    break;
  }
  case LockObjects::DROP_CAP: {
    break;
  }
  }
}