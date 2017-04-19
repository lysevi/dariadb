#pragma once

#include <libdariadb/meas.h>
#include <memory>
#include <istream>
#include <ostream>

namespace dariadb {
namespace statistic {
class IFunction {
public:
  virtual void apply(const Meas &ma) = 0;
  virtual Meas result() const = 0;
  virtual int kind() const = 0;
};
using IFunction_ptr = std::shared_ptr<IFunction>;

enum class FUNCTION_KIND : int { AVERAGE, MEDIAN, PERCENTILE90, PERCENTILE99 };

using FunctionKinds = std::vector<FUNCTION_KIND>;

EXPORT std::istream &operator>>(std::istream &in, FUNCTION_KIND &f);
EXPORT std::ostream &operator<<(std::ostream &stream, const FUNCTION_KIND &f);

} // namespace statistic
} // namespace dariadb