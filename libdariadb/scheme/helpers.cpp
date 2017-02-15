#include <libdariadb/scheme/helpers.h>

namespace dariadb {
namespace scheme {

std::ostream &operator<<(std::ostream &stream,
                         const MeasurementDescription &d) {
  stream << "{ name:" << d.name << ", id:" << d.id << "}";
  return stream;
}
}
}