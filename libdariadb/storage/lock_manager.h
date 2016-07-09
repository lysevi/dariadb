#pragma once

#include "../storage.h"
#include "../utils/utils.h"
#include "aofile.h"
#include <vector>

#include <map>
#include <mutex>
//
//#include <boost/thread/shared_mutex.hpp>

namespace dariadb {
namespace storage {

enum class LockKind : uint8_t { READ, EXCLUSIVE };

enum class LockObjects : uint8_t {
  AOF,
  CAP,
  PAGE,
  DROP_AOF,
  DROP_CAP,
};

struct MutexWrap {
  std::mutex mutex;
  LockKind kind;
  MutexWrap() : mutex(), kind(LockKind::READ) {}
};

using RWMutex_Ptr = std::shared_ptr<MutexWrap>;

class LockManager : public utils::NonCopy {
public:
  struct Params {};

protected:
  virtual ~LockManager();
  LockManager(const Params &param);

public:
  static void start(const Params &param);
  static void stop();
  static LockManager *instance();

  void lock(const LockKind &lk, const LockObjects &lo);
  void
  lock(const LockKind &lk,
       const std::vector<LockObjects> &los); /// Only simple locks support (non drop_)
  void unlock(const LockObjects &lo);
  void unlock(const std::vector<LockObjects> &los);

protected:
  RWMutex_Ptr get_or_create_lock_object(const LockObjects &lo);
  RWMutex_Ptr get_lock_object(const LockObjects &lo);
  void lock_by_kind(const LockKind &lk, const LockObjects &lo);

  void lock_drop_aof();
  void lock_drop_cap();

private:
  static LockManager *_instance;

  std::map<LockObjects, RWMutex_Ptr> _lockers;
  std::mutex _mutex;
};
}
}
