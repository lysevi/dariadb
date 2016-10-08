#include "timeutil.h"
#include <cstdio>
#include <cstring>

#define BOOST_DATE_TIME_POSIX_TIME_STD_CONFIG // to enable nanoseconds;
#include <boost/date_time/posix_time/posix_time.hpp>

namespace dariadb {
namespace timeutil {

Time from_ptime(boost::posix_time::ptime timestamp) {
  auto duration = timestamp - boost::posix_time::from_time_t(0);
  auto ns = duration.total_nanoseconds();
  return ns;
}

Time current_time() {
  auto now = boost::posix_time::microsec_clock::local_time();
  return from_ptime(now);
}

Time from_chrono(const std::chrono::high_resolution_clock::time_point &t) {
  auto now = t.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}

std::chrono::high_resolution_clock::time_point to_timepoint(Time t) {
  return std::chrono::high_resolution_clock::time_point(std::chrono::nanoseconds(t));
}

boost::posix_time::ptime to_ptime(Time timestamp) {
  boost::posix_time::ptime ptime =
      boost::posix_time::from_time_t(0) + boost::posix_time::nanoseconds(timestamp);
  return ptime;
}

DateTime to_datetime(Time t){
    using namespace boost::gregorian;
    using namespace boost::posix_time;

    auto ptime = to_ptime(t);
    auto date = ptime.date();
    auto time = ptime.time_of_day();
    auto ymd = gregorian_calendar::from_day_number(date.day_number());

    auto ns = time.fractional_seconds();

    DateTime result;
    result.year=ymd.year;
    result.month=ymd.month;
    result.day=ymd.day;
    result.hour=(uint8_t)time.hours();
    result.minute=(uint8_t)time.minutes();
    result.second=(uint8_t)time.seconds();
    result.fracsec=ns;
    return result;
}

int to_string(char *buffer, size_t buffer_size, Time t) {
  DateTime dt=to_datetime(t);

  int len = snprintf(buffer, buffer_size, "%04d-%02d-%02d %02d:%02d:%02d.%09d",
                     (int)dt.year, (int)dt.month, (int)dt.day, (int)dt.hour,
                     (int)dt.minute, (int)dt.second, (int)dt.fracsec);

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
}
}
