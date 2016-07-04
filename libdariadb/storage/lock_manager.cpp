#include "lock_manager.h"

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

LockManager *dariadb::storage::LockManager::instance() { return _instance; }

void dariadb::storage::LockManager::lock(const LockKind &lk) {}
