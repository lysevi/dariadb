#include <libdariadb/storage/engine_environment.h>

using namespace dariadb;
using namespace dariadb::storage;

struct EngineEnvironment::Private {
  Private() {}

  ~Private() {}

  void addResource(Resource res, void*ptr){

  }

  void* getResourcePtr(EngineEnvironment::Resource res)const{
      return nullptr;
  }
};

EngineEnvironment::EngineEnvironment():_impl(new EngineEnvironment::Private()) {}
EngineEnvironment::~EngineEnvironment() {
    _impl=nullptr;
}

void* EngineEnvironment::getResourcePtr(EngineEnvironment::Resource res)const{
    return _impl->getResourcePtr(res);
}

void EngineEnvironment::addResource(Resource res, void*ptr){
    _impl->addResource(res,ptr);
}
