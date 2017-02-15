#pragma once

#include <libdariadb/scheme/ischeme.h>
#include <libdariadb/st_exports.h>
#include <string>

namespace dariadb {
namespace scheme {

EXPORT std::ostream &operator<<(std::ostream &stream,
                                const MeasurementDescription &d);
}
}