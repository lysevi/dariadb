#pragma once

#include "thread_pool.h"
#include "utils.h"
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
  static void start(const Params &params);
  static void stop();
  static ThreadManager *instance();

  ~ThreadManager();
  void flush();
  TaskResult_Ptr post(const ThreadKind kind, const AsyncTask task);
private:
  ThreadManager(const Params &params);

private:
  static ThreadManager* _instance;
  Params _params;
  std::unordered_map<ThreadKind, std::shared_ptr<ThreadPool>> _pools;
};
}
}
}