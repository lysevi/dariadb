#include "timeutil.h"
#include <cstdio>
#include <cstring>

namespace dariadb {
namespace timeutil {

Time from_chrono(const std::chrono::system_clock::time_point &t) {
  auto now = t.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}

std::chrono::system_clock::time_point to_timepoint(Time t) {
  return std::chrono::system_clock::time_point(std::chrono::milliseconds(t));
}

int to_string(char *buffer, size_t buffer_size, Time t) {
  auto ns = dariadb::timeutil::to_timepoint(t);
  auto ns_c = std::chrono::system_clock::to_time_t(ns);
  auto lc = std::localtime(&ns_c);

  int len = std::snprintf(buffer, buffer_size, "%02d:%02d:%02d-%02d.%02d.%d", lc->tm_hour,
                          lc->tm_min, lc->tm_sec, lc->tm_mday, lc->tm_mon + 1,
                          1900 + lc->tm_year);

  return len;
}

std::string to_string(Time t) {
  char buffer[256];
  int buff_size = sizeof(buffer);
  std::memset(buffer, 0, buff_size);
  int res = to_string(buffer, buff_size, t);

  if (res == 0 || res == buff_size) {
    return std::string("buffer to small");
  }
  return std::string(buffer);
}

Time current_time() {
  auto now = std::chrono::system_clock::now();
  return dariadb::timeutil::from_chrono(now);
}
}
}
