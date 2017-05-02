#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <memory>

namespace dariadb {
namespace aggregator {
class ITimer {
public:
  class Callback {
  public:
    // return next calltime
    virtual Time apply(Time current_time) = 0;
  };
  using Callback_Ptr = std::shared_ptr<Callback>;

  virtual void addCallback(Time firstTime, Callback_Ptr clbk) = 0;
  virtual Time currentTime() const = 0;
};
using ITimer_Ptr = std::shared_ptr<ITimer>;
} // namespace aggregator
} // namespace dariadb