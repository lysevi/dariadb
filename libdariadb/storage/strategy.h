#pragma once

#include <istream>
#include <libdariadb/st_exports.h>
#include <ostream>

namespace dariadb {
namespace storage {

enum class STRATEGY : uint16_t { FAST_WRITE = 0, COMPRESSED, MEMORY};

EXPORT std::istream &operator>>(std::istream &in, STRATEGY &strat);
EXPORT std::ostream &operator<<(std::ostream &stream, const STRATEGY &strat);
}
}
