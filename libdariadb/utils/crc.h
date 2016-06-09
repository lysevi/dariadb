#pragma once

#include <cstddef>

namespace dariadb {
namespace utils {
size_t crc32(const void *buffer, const size_t size);
}
}
