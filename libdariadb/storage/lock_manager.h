#pragma once

#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/utils.h>
#include <libdariadb/st_exports.h>
#include <vector>

#include <map>
#include <mutex>

#include <shared_mutex>

namespace dariadb {
namespace storage {

enum class LOCK_KIND : uint8_t { UNKNOW, READ, EXCLUSIVE };

enum class LOCK_OBJECTS : uint8_t {
  AOF,
  PAGE,
  DROP_AOF,
};

struct MutexWrap {
  std::shared_mutex mutex;
  LOCK_KIND kind;
  MutexWrap() : mutex(), kind(LOCK_KIND::UNKNOW) {}
};

using RWMutex_Ptr = std::shared_ptr<MutexWrap>;

class LockManager : public utils::NonCopy {
public:
  struct Params {};

protected:
  virtual ~LockManager();
  LockManager(const Params &param);

public:
  EXPORT static void start(const Params &param);
  EXPORT static void stop();
  EXPORT static LockManager *instance();

  EXPORT void lock(const LOCK_KIND &lk, const LOCK_OBJECTS &lo);
  EXPORT void
  lock(const LOCK_KIND &lk,
       const std::vector<LOCK_OBJECTS> &los); /// Only simple locks support (non drop_)
  EXPORT void unlock(const LOCK_OBJECTS &lo);
  EXPORT void unlock(const std::vector<LOCK_OBJECTS> &los);

protected:
  RWMutex_Ptr get_or_create_lock_object(const LOCK_OBJECTS &lo);
  RWMutex_Ptr get_lock_object(const LOCK_OBJECTS &lo);
  void lock_by_kind(const LOCK_KIND &lk, const LOCK_OBJECTS &lo);

  void lock_drop_aof();
  void lock_drop_cap();

private:
  static LockManager *_instance;

  std::map<LOCK_OBJECTS, RWMutex_Ptr> _lockers;
  std::mutex _mutex;
};
}
}
