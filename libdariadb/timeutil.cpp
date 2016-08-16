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

///value to string
std::string v2s(int value){
    std::stringstream ss;
    if(value<10){
        ss<<'0';
    }
    ss<<value;
    return ss.str();
}

std::string to_string(Time t) {
  auto ns = dariadb::timeutil::to_timepoint(t);
  auto ns_c = std::chrono::system_clock::to_time_t(ns);
  auto lc = std::localtime(&ns_c);
  std::stringstream ss;
  ss << v2s(lc->tm_hour) << ":" <<v2s( lc->tm_min) << ":" << v2s(lc->tm_sec) << "-";
  ss << v2s(lc->tm_mday) << "." << v2s(lc->tm_mon + 1) << "." << 1900 + lc->tm_year;
  return ss.str();

//  auto ns = dariadb::timeutil::to_timepoint(t);
//  auto ns_c = std::chrono::system_clock::to_time_t(ns);
//  std::string ts = std::ctime(&ns_c);
//  ts.resize(ts.size()-1);
//  return ts;
}

Time current_time() {
  auto now = std::chrono::system_clock::now();
  return dariadb::timeutil::from_chrono(now);
}
}
}
