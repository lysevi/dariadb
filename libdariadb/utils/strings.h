#pragma once

#include <libdariadb/dariadb_st_exports.h>
#include <sstream>
#include <string>
#include <vector>

namespace dariadb {
namespace utils {
namespace strings {
/// split string by space.
DARIADB_ST_EXPORTS std::vector<std::string> tokens(const std::string &str);
DARIADB_ST_EXPORTS std::vector<std::string> split(const std::string &text, char sep);
DARIADB_ST_EXPORTS std::string to_upper(const std::string &text);

namespace inner {
template <class Head> void args_as_string(std::ostream &s, Head &&head) {
  s << std::forward<Head>(head);
}

template <class Head, class... Tail>
void args_as_string(std::ostream &s, Head &&head, Tail &&... tail) {
  s << std::forward<Head>(head);
  args_as_string(s, std::forward<Tail>(tail)...);
}
}
template <class... Args> std::string args_to_string(Args &&... args) {
  std::stringstream ss;
  inner::args_as_string(ss, std::forward<Args>(args)...);
  return ss.str();
}
}
}
}
