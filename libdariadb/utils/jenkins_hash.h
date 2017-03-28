#pragma once

#include <cstdint>

namespace dariadb {
namespace utils {
template <class T> uint32_t jenkins_one_at_a_time_hash(const T &value) {
  auto key = reinterpret_cast<const char *>(&value);
  auto len = sizeof(value);
  uint32_t hash, i;
  for (hash = i = 0; i < len; ++i) {
    hash += key[i];
    hash += (hash << 10);
    hash ^= (hash >> 6);
  }
  hash += (hash << 3);
  hash ^= (hash >> 11);
  hash += (hash << 15);
  return hash;
}

template <class T> struct jenkins_one_hash {
  uint32_t operator()(const T &value) const { return jenkins_one_at_a_time_hash(value); }
};

} // namespace utils
} // namespace dariadb