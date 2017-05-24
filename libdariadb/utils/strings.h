#pragma once

#include <libdariadb/st_exports.h>
#include <list>
#include <string>
#include <vector>

namespace dariadb {
namespace utils {
namespace strings {

/// split string by space.
EXPORT std::vector<std::string> tokens(const std::string &str);
EXPORT std::vector<std::string> split(const std::string &text, char sep);
EXPORT std::string to_upper(const std::string &text);
EXPORT std::string to_lower(const std::string &text);

namespace inner {
using std::to_string;
EXPORT std::string to_string(const char *_Val);
EXPORT std::string to_string(std::string &_Val);

template <class Head>
void args_as_string(std::list<std::string> &s, size_t &sz, Head &&head) {
  auto str = to_string(std::forward<Head>(head));
  sz += str.size();
  s.push_back(str);
}

template <class Head, class... Tail>
void args_as_string(std::list<std::string> &s, size_t &sz, Head &&head, Tail &&... tail) {
  auto str = to_string(std::forward<Head>(head));
  sz += str.size();
  s.push_back(str);
  args_as_string(s, sz, std::forward<Tail>(tail)...);
}
} // namespace inner

template <class... Args> std::string args_to_string(Args &&... args) {
  std::list<std::string> ss;
  size_t sz = 0;
  inner::args_as_string(ss, sz, std::forward<Args>(args)...);
  std::string result;
  result.reserve(sz);
  for (auto&&v : ss) {
    result += std::move(v);
  }
  return result;
}
} // namespace strings
} // namespace utils
} // namespace dariadb
