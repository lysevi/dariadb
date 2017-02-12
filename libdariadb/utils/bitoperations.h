#pragma once

#include <cstdint>

namespace dariadb {
namespace utils {

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

  template <class T> static inline T clr(T v, uint8_t num) { return v & ~(T(1) << num); }
};
}
}
