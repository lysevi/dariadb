#pragma once

#include "meas.h"
#include <chrono>

namespace dariadb {
    namespace timeutil {
        Time from_chrono(const std::chrono::system_clock::time_point &t);
		Time current_time();
    }
}
