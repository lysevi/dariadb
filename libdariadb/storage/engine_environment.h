#pragma once

#include <libdariadb/utils/utils.h>
#include <memory>

namespace dariadb {
namespace storage {

class EngineEnvironment : public utils::NonCopy {
public:
    enum class Resource{

    };
public:
  EngineEnvironment();
  ~EngineEnvironment();

  void addResource(Resource res, void*ptr);
  void* getResourcePtr(Resource res)const;

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
