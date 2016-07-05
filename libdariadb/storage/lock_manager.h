#pragma once

#include "../storage.h"
#include "../utils/utils.h"
#include "aofile.h"
#include <vector>

#include <mutex>
#include <map>

#include <boost/thread/shared_mutex.hpp>

namespace dariadb {
namespace storage {

    enum class LockKind:uint8_t
    {
		READ,
		EXCLUSIVE
    };

	enum class LockObjects :uint8_t
	{
		AOF,
		CAP,
		PAGE,
		DROP_AOF,
		DROP_CAP,
	};
	using RWMutex_Ptr = std::shared_ptr<boost::shared_mutex>;
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

  void lock(const LockKind&lk, const LockObjects&lo);
  void unlock(const LockObjects&lo);
protected:
	RWMutex_Ptr get_lock_object(const LockObjects&lo);
private:
  static LockManager *_instance;

  std::map<LockObjects, RWMutex_Ptr> _lockers;
  std::mutex _mutex;
};
}
}
