#pragma once

#include <istream>
#include <ostream>

namespace dariadb {
namespace storage {

enum class STRATEGY : uint16_t { DYNAMIC = 0, FAST_WRITE, FAST_READ, COMPRESSED };

std::istream &operator>>(std::istream &in, STRATEGY &strat);
std::ostream &operator<<(std::ostream &stream, const STRATEGY &strat);
}
}