#pragma once

#include <libdariadb/st_exports.h>
#include <libdariadb/storage/manifest.h>
#include <memory>

namespace dariadb {
namespace scheme {

/***
example:
-   'host1.system.memory'
-   'host1.system.cpu.bySec'
-   'host1.system.cpu.byHour'
-   query 'host1.system.*',
return [host1.system.memory, host1.system.cpu.bySec, host1.system.cpu.byHour]
*/
class Scheme {
public:
  EXPORT Scheme(const storage::Manifest_ptr m);

protected:
  class Private;
  std::unique_ptr<Private> _impl;
};
}
}