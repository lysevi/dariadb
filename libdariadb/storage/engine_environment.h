#pragma once

#include <libdariadb/st_exports.h>
#include <libdariadb/utils/utils.h>
#include <memory>

namespace dariadb {
namespace storage {

class EngineEnvironment;
using EngineEnvironment_ptr = std::shared_ptr<EngineEnvironment>;
class EngineEnvironment : public utils::NonCopy {
public:
  enum class Resource {
    // LOCK_MANAGER,
    SETTINGS,
    MANIFEST
  };

public:
  EXPORT static EngineEnvironment_ptr create();
  EXPORT ~EngineEnvironment();

  EXPORT void addResource(Resource res, void *ptr);
  EXPORT void *getResourcePtr(Resource res) const;

  template <class T> T *getResourceObject(Resource res) const {
    return (T *)getResourcePtr(res);
  }

protected:
  EXPORT EngineEnvironment();

protected:
  struct Private;
  std::unique_ptr<Private> _impl;
};
}
}
