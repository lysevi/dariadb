#pragma once

#include "thread_pool.h"
#include "utils.h"
#include <unordered_map>

namespace dariadb {
namespace utils {
namespace async {
	enum class THREAD_COMMON_KINDS : ThreadKind {
		READ = 1
	};

	const std::vector<ThreadPool::Params> THREAD_MANAGER_COMMON_PARAMS{
		ThreadPool::Params{size_t(5), (ThreadKind)THREAD_COMMON_KINDS::READ}
	};

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
  TaskResult_Ptr post(const THREAD_COMMON_KINDS kind, const AsyncTaskWrap& task) {
	  return this->post((ThreadKind)kind, task);
  }
  TaskResult_Ptr post(const ThreadKind kind, const AsyncTaskWrap& task);
private:
  ThreadManager(const Params &params);

private:
  static ThreadManager* _instance;
  bool _stoped;
  Params _params;
  std::unordered_map<ThreadKind, std::shared_ptr<ThreadPool>> _pools;
};
}
}
}
