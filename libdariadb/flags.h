#pragma once

#include <libdariadb/meas.h>
#include <stdint.h>
namespace dariadb {
namespace FLAGS {

const Flag _NO_DATA = MAX_FLAG; // {1111 1111, 1111 1111, 1111 1111, 1111 1111}
const Flag _STATS = 0x80000000; // {1000 0000, 0000 0000, 0000 0000, 0000 0000}

} // namespace FLAGS
} // namespace dariadb
