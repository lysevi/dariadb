#pragma once

#include "meas.h"
#include <chrono>

namespace dariadb {
namespace timeutil {
Time from_chrono(const std::chrono::system_clock::time_point &t);
std::chrono::system_clock::time_point to_timepoint(Time t);
std::string to_string(Time t);
Time current_time();
}
}
