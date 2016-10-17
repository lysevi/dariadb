#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/dariadb_st_exports.h>
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
DARIADB_ST_EXPORTS Time current_time();

///convert time to datetime;
DARIADB_ST_EXPORTS DateTime to_datetime(Time t);

/// convert from time_point
DARIADB_ST_EXPORTS Time from_chrono(const std::chrono::high_resolution_clock::time_point &t);

/// convert to time_point
DARIADB_ST_EXPORTS std::chrono::high_resolution_clock::time_point to_timepoint(Time t);

/// convert to string
DARIADB_ST_EXPORTS int to_string(char *buffer, size_t buffer_size, Time t);

/// convert to string
DARIADB_ST_EXPORTS std::string to_string(Time t);
}
}
