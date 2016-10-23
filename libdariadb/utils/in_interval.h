#pragma once

namespace dariadb {
namespace utils {

template <typename T> bool inInterval(const T from, const T to, const T value) {
  return value >= from && value <= to;
}

template <typename T>
bool intervalsIntersection(const T from1, const T to1, const T from2, const T to2) {
  return inInterval(from1, to1, from2) || inInterval(from1, to1, to2) ||
         inInterval(from2, to2, from1) || inInterval(from2, to2, to1);
}
}
}
