#pragma once

#include <cstdint>
#include <iterator>
#include <libdariadb/utils/exception.h>
#include <list>
#include <string>
#include <vector>

#define NOT_IMPLEMENTED THROW_EXCEPTION("Not implemented");

#ifdef DOUBLE_CHECKS
#define ENSURE_MSG(A, E)                                                       \
  if (!(A)) {                                                                  \
    THROW_EXCEPTION(E);                                                        \
  }
#define ENSURE(A) ENSURE_MSG(A, "check failed")
#define ENSURE_NOT_NULL(A) ENSURE_MSG(A, "null pointer")
#else
#define ENSURE_MSG(A)
#define ENSURE(A)
#define ENSURE_NOT_NULL(A)
#endif

namespace dariadb {
namespace utils {

// TODO move to file.
struct BitOperations {
  template <class T> static inline uint8_t get(T v, uint8_t num) {
    return (v >> num) & 1;
  }

  template <class T> static inline bool check(T v, uint8_t num) {
    return get(v, num) == 1;
  }

  template <class T> static inline T set(T v, uint8_t num) {
    return v | (static_cast<T>(T(1) << num));
  }

  template <class T> static inline T clr(T v, uint8_t num) {
    return v & ~(T(1) << num);
  }
};

class NonCopy {
private:
  NonCopy(const NonCopy &) = delete;
  NonCopy &operator=(const NonCopy &) = delete;

protected:
  NonCopy() = default;
};

///TODO move to bytebuffer.h file
struct Range {
  uint8_t *begin;
  uint8_t *end;
  Range() { begin = end = nullptr; }

  Range(uint8_t *_begin, uint8_t *_end) {
    begin = _begin;
    end = _end;
  }
};
}
}
