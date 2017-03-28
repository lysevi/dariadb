#pragma once

#include <libdariadb/utils/jenkins_hash.h>

namespace dariadb {
namespace storage {

template <typename T> static uint64_t bloom_empty() {
  return uint64_t(0);
}

template <typename T> static uint64_t bloom_add(const uint64_t fltr, const T &val) {
  auto h = utils::jenkins_one_at_a_time_hash(val);
  return fltr | h;
}

template <typename T> static bool bloom_check(const uint64_t fltr, const T &val) {
  auto h = utils::jenkins_one_at_a_time_hash(val);
  return (fltr & h) == h;
}

static uint64_t bloom_combine(const uint64_t fltr_a, const uint64_t fltr_b) {
  return fltr_a | fltr_b;
}
} // namespace storage
} // namespace dariadb
