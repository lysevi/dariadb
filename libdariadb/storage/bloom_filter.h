#pragma once

#include <cassert>
#include <functional>

namespace dariadb {
namespace storage {

template <typename T> static uint64_t bloom_empty() {
  return size_t();
}

template <typename T> static uint64_t bloom_add(const uint64_t &fltr, const T &val) {
  auto h = static_cast<uint64_t>(std::hash<T>()(val));
  return fltr | h;
}

template <typename T> static bool bloom_check(const uint64_t &fltr, const T &val) {
  auto h = static_cast<uint64_t>(std::hash<T>()(val));
  return (fltr & h) == h;
}
}
}
