#pragma once

#include <libdariadb/st_exports.h>
#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/utils.h>
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
public:
  EXPORT virtual ~LockManager();
  EXPORT LockManager(const Params &param);

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
  std::map<LOCK_OBJECTS, RWMutex_Ptr> _lockers;
  std::mutex _mutex;
};

using LockManager_ptr=std::shared_ptr<LockManager>;
}
}
