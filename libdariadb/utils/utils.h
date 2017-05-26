#pragma once

#include <libdariadb/utils/exception.h>

#define NOT_IMPLEMENTED THROW_EXCEPTION("Not implemented");

#ifdef DOUBLE_CHECKS
#define ENSURE_MSG(A, E)                                                                 \
  if (!(A)) {                                                                            \
    THROW_EXCEPTION(E);                                                                  \
  }
#define ENSURE(A) ENSURE_MSG(A, #A)
#else
#define ENSURE_MSG(A, E)
#define ENSURE(A)
#endif

namespace dariadb {
namespace utils {

inline void sleep_mls(long long a) {
  std::this_thread::sleep_for(std::chrono::milliseconds(a));
}

class NonCopy {
private:
  NonCopy(const NonCopy &) = delete;
  NonCopy &operator=(const NonCopy &) = delete;

protected:
  NonCopy() = default;
};

struct ElapsedTime {
  ElapsedTime() { start_time = clock(); }

  double elapsed() { return double(clock() - start_time) / CLOCKS_PER_SEC; }
  clock_t start_time;
};

} // namespace utils
} // namespace dariadb
