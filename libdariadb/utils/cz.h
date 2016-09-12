#pragma once

#include <cstdint>
#ifdef MSVC
#include <intrin.h>
#define NOMINMAX
#include <windows.h>
#endif

namespace dariadb {
namespace utils {
#ifdef MSVC
uint8_t __inline ctz(uint64_t value) {
  DWORD trailing_zero = 0;

  if (_BitScanForward64(&trailing_zero, value)) {
    return static_cast<uint8_t>(trailing_zero);
  } else {
    return uint8_t(64);
  }
}

uint8_t __inline clz(uint64_t value) {
  DWORD leading_zero = 0;

  if (_BitScanReverse64(&leading_zero, value)) {
    return static_cast<uint8_t>(63 - leading_zero);
  } else {
    return uint8_t(64);
  }
}
#elif defined(GNU_CPP) || defined(CLANG_CPP)
inline uint8_t clz(uint64_t x) {
  return static_cast<uint8_t>(__builtin_clzll(x));
}
inline uint8_t ctz(uint64_t x) {
  return static_cast<uint8_t>(__builtin_ctzll(x));
}
#endif
}
}
