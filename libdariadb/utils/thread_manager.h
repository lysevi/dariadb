#pragma once

#include "thread_pool.h"
#include "utils.h"
#include <unordered_map>

namespace dariadb {
namespace utils {
namespace async {
	enum class THREAD_COMMON_KINDS : ThreadKind {
        READ = 1,
        FILE_READ,
		DROP
	};

	const std::vector<ThreadPool::Params> THREAD_MANAGER_COMMON_PARAMS{
        ThreadPool::Params{size_t(4), (ThreadKind)THREAD_COMMON_KINDS::READ},
        ThreadPool::Params{size_t(3), (ThreadKind)THREAD_COMMON_KINDS::FILE_READ},
		ThreadPool::Params{size_t(1), (ThreadKind)THREAD_COMMON_KINDS::DROP}
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

  size_t active_works(){
      size_t res=0;
      for(auto&kv:_pools){
          res+=kv.second->active_works();
      }
      return res;
  }
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
