#pragma once

#include <cstddef>
#include <cstdint>

namespace dariadb {
namespace utils {
uint32_t crc32(const void *buffer, const size_t size);
}
}
