#pragma once

#include <libdariadb/meas.h>

namespace dariadb {
namespace statistic {
class IFunction {
public:
  virtual void apply(const Meas &ma) = 0;
  virtual Meas result()const = 0;
};
} // namespace statistic
} // namespace dariadb