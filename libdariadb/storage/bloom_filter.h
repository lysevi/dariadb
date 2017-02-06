#pragma once

#include <cstdint>

namespace dariadb {
namespace storage {

template <class T> uint32_t jenkins_one_at_a_time_hash(T &value) {
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

template <typename T> static uint64_t bloom_empty() {
  return uint64_t(0);
}

template <typename T> static uint64_t bloom_add(const uint64_t fltr, const T &val) {
  auto h = jenkins_one_at_a_time_hash(val);
  return fltr | h;
}

template <typename T> static bool bloom_check(const uint64_t fltr, const T &val) {
  auto h = jenkins_one_at_a_time_hash(val);
  return (fltr & h) == h;
}

static uint64_t bloom_combine(const uint64_t fltr_a, const uint64_t fltr_b) {
  return fltr_a | fltr_b;
}
}
}
