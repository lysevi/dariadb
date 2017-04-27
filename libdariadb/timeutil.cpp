#include <libdariadb/timeutil.h>
#include <cstdio>
#include <cstring>

#include <boost/date_time/posix_time/posix_time.hpp>

namespace dariadb {
namespace timeutil {

const boost::posix_time::ptime START = boost::posix_time::from_time_t(0);

Time from_ptime(boost::posix_time::ptime timestamp) {
  auto duration = timestamp - START;
  auto ns = duration.total_milliseconds();
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
  boost::posix_time::ptime ptime = START + boost::posix_time::milliseconds(timestamp);
  return ptime;
}

DateTime to_datetime(Time t) {
  using namespace boost::gregorian;
  using namespace boost::posix_time;

  auto ptime = to_ptime(t);
  auto date = ptime.date();
  auto time = ptime.time_of_day();
  auto ymd = gregorian_calendar::from_day_number(date.day_number());

  DateTime result;
  result.year = ymd.year;
  result.month = (uint8_t)ymd.month;
  result.day = ymd.day;
  result.day_of_year = date.day_of_year();
  result.hour = (uint8_t)time.hours();
  result.minute = (uint8_t)time.minutes();
  result.second = (uint8_t)time.seconds();
  result.millisecond = (uint16_t)(time.total_milliseconds() % 1000);
  return result;
}

int to_string(char *buffer, size_t buffer_size, Time t) {
  DateTime dt = to_datetime(t);

  int len = snprintf(buffer, buffer_size, "%04d-%02d-%02d %02d:%02d:%02d.%04d",
                     (int)dt.year, (int)dt.month, (int)dt.day, (int)dt.hour,
                     (int)dt.minute, (int)dt.second, (int)dt.millisecond);

  return len;
}

std::string to_string(Time t) {
  char buffer[512];
  int buff_size = sizeof(buffer);
  std::memset(buffer, 0, buff_size);
  int res = to_string(buffer, buff_size, t);

  if (res == 0 || res == buff_size) {
    return std::string("buffer to small");
  }
  return std::string(buffer);
}

/// construct from string "2002-01-20 23:59:59.000"
Time from_string(const std::string &s) {
  auto pt = boost::posix_time::time_from_string(s);
  return from_ptime(pt);
}

/// construct from string "20020131T235959"
Time from_iso_string(const std::string &s) {
  auto pt = boost::posix_time::from_iso_string(s);
  return from_ptime(pt);
}

Time from_days(const int day) {
  return from_hours(day * 24);
}

Time from_hours(const int h) {
  return from_minutes(h * 60);
}

Time from_minutes(const int m) {
  return from_seconds(m * 60);
}

Time from_seconds(const int s) {
  return Time(s) * 1000;
}

Time round_to_seconds(const Time t) {
  return (Time)(Time(float(t) / 1e3) * 1e3);
}

Time round_to_minutes(const Time t) {
  return t - (t % (60 * 1000));
}

Time round_to_hours(const Time t) {
  return t - (t % (3600 * 1000));
}

Time intervalName2time(const std::string &interval) {
  const Time minute = 1000 * 60;
  const Time hour = minute * 60;
  const Time day = hour * 24;
  if (interval == "minute") {
    return minute;
  }
  if (interval == "halfhour") {
    return minute * 30;
  }
  if (interval == "hour") {
    return hour;
  }
  if (interval == "day") {
    return day;
  }
  if (interval == "week") {
    return day * 7;
  }
  if (interval == "month31") {
    return day * 31;
  }
  return 0;
}

bool intervalsLessCmp(const std::string &l, const std::string &r) {
  auto ltime = intervalName2time(l);
  auto rtime = intervalName2time(r);
  return ltime < rtime;
}

std::vector<std::string> predefinedIntervals() {
  std::vector<std::string> predefined = {"minute", "halfhour", "hour", "day", "week", "month31"};
  return predefined;
}
} // namespace timeutil
} // namespace dariadb
