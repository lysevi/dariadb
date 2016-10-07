#pragma once

#include "meas.h"
#include <chrono>


namespace dariadb {
namespace timeutil {

/// current timestamp with nanosecond.
Time current_time();

/// convert from time_point
Time from_chrono(const std::chrono::high_resolution_clock::time_point &t);

/// convert to time_point
std::chrono::high_resolution_clock::time_point to_timepoint(Time t);

/// convert to string
int to_string(char *buffer, size_t buffer_size, Time t);

/// convert to string
std::string to_string(Time t);
}
}
