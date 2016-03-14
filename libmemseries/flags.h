#pragma once

#include <stdint.h>

namespace memseries {
	enum Flags : uint64_t {
		NO_DATA = 0xffffffffffffffff
	};
}