#pragma once

#include <libdariadb/aggregate/timer.h>
#include <libdariadb/interfaces/iengine.h>
#include <libdariadb/st_exports.h>
#include <memory>

namespace dariadb {
namespace aggregate {
class Aggreagator {
public:
  EXPORT Aggreagator();

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};
} // namespace aggregate
} // namespace dariadb