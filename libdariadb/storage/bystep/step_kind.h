#pragma once

#include <libdariadb/st_exports.h>
#include <libdariadb/meas.h>
#include <istream>
#include <ostream>

namespace dariadb {
namespace storage {

enum class STEP_KIND { SECOND, MINUTE, HOUR };

using Id2Step = std::unordered_map<Id, STEP_KIND>;

EXPORT std::istream &operator>>(std::istream &in, STEP_KIND &strat);
EXPORT std::ostream &operator<<(std::ostream &stream, const STEP_KIND &strat);

EXPORT Time stepByKind(const STEP_KIND stepkind);
}
}