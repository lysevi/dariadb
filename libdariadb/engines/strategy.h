#pragma once

#include <libdariadb/st_exports.h>
#include <istream>
#include <ostream>

namespace dariadb {
enum class STRATEGY : uint16_t { WAL = 0, COMPRESSED, MEMORY, CACHE };

EXPORT std::istream &operator>>(std::istream &in, STRATEGY &strat);
EXPORT std::ostream &operator<<(std::ostream &stream, const STRATEGY &strat);

EXPORT std::string to_string(const STRATEGY &strat);
}

