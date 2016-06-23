#pragma once

#include <cassert>
#include <functional>

namespace dariadb {
namespace storage {

template <typename T> static size_t bloom_empty() { return size_t(); }

template <typename T>
static size_t bloom_add(const size_t &fltr, const T &val) {
  auto h = std::hash<T>()(val);
  return fltr | h;
}

template <typename T>
static bool bloom_check(const size_t &fltr, const T &val) {
  //assert(fltr != size_t(0));
  auto h = std::hash<T>()(val);
  return (fltr & h) == h;
}
}
}
