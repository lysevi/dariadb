#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <chrono>

namespace dariadb {
namespace timeutil {

struct DateTime {
  uint16_t year;
  uint8_t month;
  uint16_t day;
  uint16_t day_of_year;
  uint8_t hour;
  uint8_t minute;
  uint8_t second;
  uint16_t millisecond;
};

/// current timestamp with nanosecond.
EXPORT Time current_time();

///convert time to datetime;
EXPORT DateTime to_datetime(Time t);

/// convert from time_point
EXPORT Time from_chrono(const std::chrono::high_resolution_clock::time_point &t);

/// convert to time_point
EXPORT std::chrono::high_resolution_clock::time_point to_timepoint(Time t);

/// convert to string
EXPORT int to_string(char *buffer, size_t buffer_size, Time t);

/// convert to string
EXPORT std::string to_string(Time t);
}
}
