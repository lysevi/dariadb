#pragma once

#include <istream>
#include <ostream>
#include <libdariadb/dariadb_st_exports.h>

namespace dariadb {
namespace storage {

enum class STRATEGY : uint16_t { FAST_WRITE=0, COMPRESSED };

DARIADB_ST_EXPORTS std::istream &operator>>(std::istream &in, STRATEGY &strat);
DARIADB_ST_EXPORTS std::ostream &operator<<(std::ostream &stream, const STRATEGY &strat);
}
}
