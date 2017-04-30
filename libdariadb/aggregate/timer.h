#pragma once
#include <libdariadb/aggregate/itimer.h>

namespace dariadb {
namespace aggregator {
class Timer : public ITimer {
public:
	EXPORT Timer();
};
} // namespace aggregator
} // namespace dariadb