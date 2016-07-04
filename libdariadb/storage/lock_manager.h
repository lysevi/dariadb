#pragma once

#include "../storage.h"
#include "../utils/utils.h"
#include "aofile.h"
#include <vector>

#include <mutex>

namespace dariadb {
namespace storage {

    enum class LockKind:uint8_t
    {

    };
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

  void lock(const LockKind&lk);
private:
  static LockManager *_instance;
};
}
}
