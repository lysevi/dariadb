#pragma once

#include <istream>
#include <ostream>
#include <libdariadb/st_exports.h>

namespace dariadb {
namespace storage {

enum class STRATEGY : uint16_t { FAST_WRITE=0, COMPRESSED };

EXPORT std::istream &operator>>(std::istream &in, STRATEGY &strat);
EXPORT std::ostream &operator<<(std::ostream &stream, const STRATEGY &strat);
}
}
