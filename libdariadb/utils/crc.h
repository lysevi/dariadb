#pragma once

#include <libdariadb/st_exports.h>
#include <cstddef>
#include <cstdint>

namespace dariadb {
namespace utils {
EXPORT uint32_t crc32(const void *buffer, const size_t size);
EXPORT uint16_t crc16(const void *buffer, const size_t size);
}
}
