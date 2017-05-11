#include <libdariadb/scheme/helpers.h>

namespace dariadb {
namespace scheme {

std::ostream &operator<<(std::ostream &stream, const MeasurementDescription &d) {
  stream << "{ name:" << d.name << ", id:" << d.id;

  if (!d.interval.empty()) {
    stream << ", interval:" << d.interval;
  }

  if (!d.aggregation_func.empty()) {
    stream << ", func:" << d.aggregation_func;
  }

  stream << "}";
  return stream;
}
} // namespace scheme
} // namespace dariadb