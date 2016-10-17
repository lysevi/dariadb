#pragma once

#include <libdariadb/utils/thread_pool.h>
#include <libdariadb/utils/utils.h>
#include <libdariadb/dariadb_st_exports.h>
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
  DARIADB_ST_EXPORTS static void start(const Params &params);
  DARIADB_ST_EXPORTS static void stop();
  DARIADB_ST_EXPORTS static ThreadManager *instance();

  DARIADB_ST_EXPORTS ~ThreadManager();
  DARIADB_ST_EXPORTS void flush();
  TaskResult_Ptr post(const THREAD_COMMON_KINDS kind, const AsyncTaskWrap &task) {
    return this->post((ThreadKind)kind, task);
  }
  DARIADB_ST_EXPORTS TaskResult_Ptr post(const ThreadKind kind, const AsyncTaskWrap &task);

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
