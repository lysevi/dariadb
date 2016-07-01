#pragma once

#include "utils.h"
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace dariadb {
namespace utils {
namespace async {

using ThreadKind = uint16_t;

#ifdef DEBUG
#define TKIND_CHECK(expected, exists)                                                    \
  if ((ThreadKind)expected != exists) {                                                              \
    throw MAKE_EXCEPTION("wrong thread kind");                                           \
  }
#else //  DEBUG
#define TKIND_CHECK(expected, exists)                                                    \
  (void)(expected);                                                                      \
  (void)(exists);
#endif

struct ThreadInfo {
  ThreadKind kind;
  size_t thread_number;
};

using AsyncTask = std::function<void(const ThreadInfo &)>;
using TaskQueue = std::deque<AsyncTask>;

struct TaskResult {
  bool runned;
  std::condition_variable _cv;

  TaskResult() { runned = true; }
  ~TaskResult() {}
  void wait() {
    std::mutex _mutex;

    std::unique_lock<std::mutex> ul(_mutex);
    _cv.wait(ul, [&] { return !runned; });
  }

  void unlock() {
    runned = false;
    _cv.notify_one();
  }
};
using TaskResult_Ptr = std::shared_ptr<TaskResult>;

class ThreadPool : public utils::NonCopy {
public:
  struct Params {
    size_t threads_count;
    ThreadKind kind;
    Params(size_t _threads_count, ThreadKind _kind) {
      threads_count = _threads_count;
      kind = _kind;
    }
  };
  ThreadPool(const Params &p);
  ~ThreadPool();
  size_t threads_count() const { return _params.threads_count; }
  ThreadKind kind() const { return _params.kind; }

  bool is_stoped() const { return _is_stoped; }

  TaskResult_Ptr post(const AsyncTask task);
  void flush();
  void stop();

protected:
  void _thread_func(size_t num);

protected:
  Params _params;
  std::vector<std::thread> _threads;
  std::mutex _locker;
  TaskQueue _in_queue;
  std::condition_variable _data_cond;
  bool _stop_flag, _is_stoped;
};
}
}
}
