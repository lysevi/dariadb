#pragma once

#include "meas.h"
#include <chrono>

namespace memseries {
namespace timeutil {
Time from_chrono(const std::chrono::system_clock::time_point &t);
}
}
