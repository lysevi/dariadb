#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/utils/exception.h>
#include <unordered_map>
using namespace dariadb;
using namespace dariadb::storage;

struct EngineEnvironment::Private {
  Private() {}

  ~Private() {}

  void addResource(Resource res, void *ptr) { _resource_map[res] = ptr; }

  void *getResourcePtr(EngineEnvironment::Resource res) const {
    auto fres = _resource_map.find(res);
    if (fres == _resource_map.end()) {
      THROW_EXCEPTION("unknow resource: ", (int)res);
    }
    return fres->second;
  }

  std::unordered_map<Resource, void *> _resource_map;
};

EngineEnvironment::EngineEnvironment() : _impl(new EngineEnvironment::Private()) {}
EngineEnvironment::~EngineEnvironment() {
  _impl = nullptr;
}

void *EngineEnvironment::getResourcePtr(EngineEnvironment::Resource res) const {
  return _impl->getResourcePtr(res);
}

void EngineEnvironment::addResource(Resource res, void *ptr) {
  _impl->addResource(res, ptr);
}
