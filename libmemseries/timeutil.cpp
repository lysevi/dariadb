#include "timeutil.h"

namespace memseries {
    namespace timeutil {
        Time from_chrono(const std::chrono::system_clock::time_point&t){
            auto now=t.time_since_epoch();
            return std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
        }
    }
}
