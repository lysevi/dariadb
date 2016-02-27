#include "timeutil.h"

memseries::Time memseries::timeutil::from_chrono(const std::chrono::system_clock::time_point&t){
    auto now=t.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
}
