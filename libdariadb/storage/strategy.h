#pragma once

#include <istream>
#include <libdariadb/st_exports.h>
#include <ostream>

namespace dariadb {
namespace storage {

enum class STRATEGY : uint16_t { WAL = 0, COMPRESSED, MEMORY, CACHE };

EXPORT std::istream &operator>>(std::istream &in, STRATEGY &strat);
EXPORT std::ostream &operator<<(std::ostream &stream, const STRATEGY &strat);

EXPORT std::string to_string(const STRATEGY &strat);
}
}
