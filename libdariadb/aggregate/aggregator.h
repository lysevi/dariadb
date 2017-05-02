#pragma once

#include <libdariadb/aggregate/timer.h>
#include <libdariadb/interfaces/iengine.h>
#include <libdariadb/st_exports.h>
#include <memory>

namespace dariadb {
namespace aggregator {
class Aggreagator {
public:
  EXPORT Aggreagator();

  EXPORT static void aggregate(const std::string &from_interval,
                               const std::string &to_interval, IEngine_Ptr engine,
                               Time start, Time end);

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};
} // namespace aggregator
} // namespace dariadb