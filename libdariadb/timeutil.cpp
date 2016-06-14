#include "timeutil.h"
#include <iomanip>
#include <sstream>

namespace dariadb {
namespace timeutil {

Time from_chrono(const std::chrono::system_clock::time_point &t) {
  auto now = t.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}

std::chrono::system_clock::time_point to_timepoint(Time t) {
  return std::chrono::system_clock::time_point(std::chrono::milliseconds(t));
}

std::string to_string(Time t) {
  auto ns = dariadb::timeutil::to_timepoint(t);
  auto ns_c = std::chrono::system_clock::to_time_t(ns);
  auto lc = std::localtime(&ns_c);
  std::stringstream ss;
  ss << lc->tm_hour << ":" << lc->tm_min << ":" << lc->tm_sec << " ";
  ss << lc->tm_mday << "." << lc->tm_mon + 1 << "." << 1900 + lc->tm_year;
  return ss.str();
}

Time current_time() {
  auto now = std::chrono::system_clock::now();
  return dariadb::timeutil::from_chrono(now);
}
}
}
