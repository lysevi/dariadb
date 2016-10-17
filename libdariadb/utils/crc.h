#pragma once

#include <libdariadb/dariadb_st_exports.h>
#include <cstddef>
#include <cstdint>

namespace dariadb {
namespace utils {
DARIADB_ST_EXPORTS uint32_t crc32(const void *buffer, const size_t size);
DARIADB_ST_EXPORTS uint16_t crc16(const void *buffer, const size_t size);
}
}
