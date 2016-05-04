#pragma once

#include "locker.h"
#include <atomic>
#include <memory>
#include <queue>

namespace dariadb {
namespace utils {
/// Object Pool
class Pool {
public:
  Pool(size_t max_size);
  ~Pool();
  void *alloc(std::size_t sz);
  void free(void *ptr, std::size_t sz);
  size_t polled();

private:
  std::queue<void *> _ptrs;
  size_t _max_size;
  Locker _locker;
};
}
}
