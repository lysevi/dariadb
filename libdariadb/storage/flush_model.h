#pragma once

#include <libdariadb/st_exports.h>
#include <istream>
#include <ostream>

namespace dariadb {
namespace storage {
enum class FlushModel : uint16_t {
  FULL = 0,   // sync every times on write
  NOT_SAFETY, // dont sync. only when close.
};

EXPORT std::istream &operator>>(std::istream &in, FlushModel &fm);
EXPORT std::ostream &operator<<(std::ostream &stream, const FlushModel &fm);

EXPORT std::string to_string(const FlushModel &fm);
}
}