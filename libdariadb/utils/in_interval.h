#pragma once

namespace dariadb {
namespace utils {

template <typename T> bool inInterval(const T from, const T to, const T value) {
  return value >= from && value <= to;
}
}
}