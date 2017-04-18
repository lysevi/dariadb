#pragma once

#include <libdariadb/meas.h>
#include <memory>

namespace dariadb {
namespace statistic {
class IFunction {
public:
  virtual void apply(const Meas &ma) = 0;
  virtual Meas result() const = 0;
  virtual int kind() const = 0;
};
using IFunction_ptr = std::shared_ptr<IFunction>;
} // namespace statistic
} // namespace dariadb