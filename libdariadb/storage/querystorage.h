#pragma once

#include <libdariadb/st_exports.h>
#include <libdariadb/interfaces/imeassource.h>
#include <libdariadb/meas.h>
#include <memory>

namespace dariadb {
namespace storage {
class QueryStorage {
public:
  struct Params {};

public:
  EXPORT QueryStorage(const Params &p);
  EXPORT ~QueryStorage();

  EXPORT Id begin(const Time step, const Id meas_id, const Time from, const Time to);
  EXPORT append_result append(const Time step, const Meas &value);

private:
  struct Private;
  std::unique_ptr<Private> _impl;
};
}
}
