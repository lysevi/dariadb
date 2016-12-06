#pragma once

#include <libdariadb/utils/utils.h>
#include <libdariadb/st_exports.h>
#include <memory>

namespace dariadb {
namespace storage {

class EngineEnvironment : public utils::NonCopy {
public:
    enum class Resource{
		//LOCK_MANAGER,
		SETTINGS,
		MANIFEST
    };
public:
  EXPORT EngineEnvironment();
  EXPORT ~EngineEnvironment();

  EXPORT void addResource(Resource res, void*ptr);
  EXPORT void* getResourcePtr(Resource res)const;

  template<class T>
  T* getResourceObject(Resource res)const{
      return (T*)getResourcePtr(res);
  }
protected:
  struct Private;
  std::unique_ptr<Private> _impl;
};

using EngineEnvironment_ptr=std::shared_ptr<EngineEnvironment>;
}
}
