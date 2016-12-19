#pragma once

#include <libdariadb/utils/async/thread_pool.h>
#include <libdariadb/utils/utils.h>
#include <libdariadb/st_exports.h>
#include <unordered_map>

namespace dariadb {
namespace utils {
namespace async {

class ThreadManager : public utils::NonCopy {

public:
  struct Params {
    std::vector<ThreadPool::Params> pools;
    Params(std::vector<ThreadPool::Params> _pools) { pools = _pools; }
  };
  EXPORT static void start(const Params &params);
  EXPORT static void stop();
  EXPORT static ThreadManager *instance();

  EXPORT ~ThreadManager();
  EXPORT void flush();
  TaskResult_Ptr post(const THREAD_KINDS kind, const std::shared_ptr<AsyncTaskWrap> &task) {
    return this->post((ThreadKind)kind, task);
  }
  EXPORT TaskResult_Ptr post(const ThreadKind kind, const AsyncTaskWrap_Ptr &task);

  size_t active_works() {
    size_t res = 0;
    for (auto &kv : _pools) {
      res += kv.second->active_works();
    }
    return res;
  }

private:
  ThreadManager(const Params &params);

private:
  static ThreadManager *_instance;
  bool _stoped;
  Params _params;
  std::unordered_map<ThreadKind, std::shared_ptr<ThreadPool>> _pools;
};
}
}
}
