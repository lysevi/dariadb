#pragma once

#include <libdariadb/scheme/helpers.h>
#include <libdariadb/scheme/ischeme.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/settings.h>
#include <memory>

namespace dariadb {
namespace scheme {

/***
example:
* memory.raw
* memory.median.hour
* memory.sigma.hour
* memory.sigma.'T\d{19}'
* memory.median.day
* memory.median.month
*/
class Scheme : public IScheme {
public:
  EXPORT static IScheme_Ptr create(const storage::Settings_ptr s);
  EXPORT Scheme(const storage::Settings_ptr s);
  EXPORT Id addParam(const std::string &param) override;
  EXPORT DescriptionMap ls() override;
  EXPORT MeasurementDescription descriptionFor(dariadb::Id id) override;
  EXPORT DescriptionMap lsInterval(const std::string &interval) override;
  EXPORT DescriptionMap linkedForValue(const MeasurementDescription &param) override;
  EXPORT void save();

protected:
  struct Private;
  std::unique_ptr<Private> _impl;
};
}
}
