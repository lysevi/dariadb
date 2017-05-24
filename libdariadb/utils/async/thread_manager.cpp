#include <libdariadb/utils/async/thread_manager.h>
#include <libdariadb/utils/exception.h>

using namespace dariadb::utils::async;

ThreadManager *ThreadManager::_instance = nullptr;

void ThreadManager::start(const ThreadManager::Params &params) {
  if (_instance == nullptr) {
    _instance = new ThreadManager(params);
  }
}

void ThreadManager::stop() {
  delete _instance;
  _instance = nullptr;
}

ThreadManager *ThreadManager::instance() {
  return _instance;
}

ThreadManager::ThreadManager(const ThreadManager::Params &params) : _params(params) {
  for (const auto&kv : _params.pools) {
    _pools[kv.kind] = std::make_shared<ThreadPool>(kv);
  }
  _stoped = false;
}

void ThreadManager::flush() {
  for (auto &kv : _pools) {
    kv.second->flush();
  }
}

TaskResult_Ptr ThreadManager::post(const ThreadKind kind, const AsyncTaskWrap_Ptr &task) {
  auto target = _pools.find(kind);
  if (target == _pools.end()) {
    throw MAKE_EXCEPTION("unknow kind.");
  }
  return target->second->post(task);
}

ThreadManager::~ThreadManager() {
  if (!_stoped) {
    for (auto&&kv : _pools) {
      kv.second->stop();
    }
    _pools.clear();
    _stoped = true;
  }
}
