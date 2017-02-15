#pragma once

#include <libdariadb/scheme/ischeme.h>
#include <libdariadb/scheme/helpers.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/settings.h>
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
class Scheme;
using Scheme_Ptr = std::shared_ptr<Scheme>;

class Scheme : public IScheme {
public:
  EXPORT static Scheme_Ptr create(const storage::Settings_ptr s);

  EXPORT Id addParam(const std::string &param) override;
  EXPORT DescriptionMap ls() override;
  EXPORT void save();
protected:
  EXPORT Scheme(const storage::Settings_ptr s);
  struct Private;
  std::unique_ptr<Private> _impl;
};
}
}
