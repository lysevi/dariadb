#pragma once

#include <libdariadb/utils/locker.h>
#include <libdariadb/utils/utils.h>
#include <atomic>
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

enum class THREAD_COMMON_KINDS : ThreadKind { DISK_IO = 1, COMMON, DROP };

#ifdef DEBUG
#define TKIND_CHECK(expected, exists)                                                    \
  if ((ThreadKind)expected != exists) {                                                  \
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

struct AsyncTaskWrap {
  AsyncTask task;
  std::string parent_function;
  std::string code_file;
  int code_line;
  AsyncTaskWrap() = default;
  AsyncTaskWrap(AsyncTask &t, std::string _function, std::string file, int line) {
    task = t;
    parent_function = _function;
    code_file = file;
    code_line = line;
  }
};

#define AT(task)                                                                         \
  AsyncTaskWrap(task, std::string(__FUNCTION__), std::string(__FILE__), __LINE__)

using TaskQueue = std::deque<AsyncTaskWrap>;

struct TaskResult {
  bool runned;
  Locker
      m; // dont use mutex. mutex::lock() requires that the calling thread owns the mutex.
  TaskResult() {
    runned = true;
    m.lock();
  }
  ~TaskResult() {}
  void wait() {
    m.lock();
    m.unlock();
  }

  void unlock() {
    runned = false;
    m.unlock();
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

  TaskResult_Ptr post(const AsyncTaskWrap &task);
  void flush();
  void stop();

  size_t active_works() {
    size_t res = _in_queue.size();
    return res + (_task_runned);
  }

protected:
  void _thread_func(size_t num);

protected:
  Params _params;
  std::vector<std::thread> _threads;
  TaskQueue _in_queue;
  std::mutex _queue_mutex;
  std::condition_variable _condition;
  bool _stop_flag;                 // true - pool under stop.
  bool _is_stoped;                 // true - already stopped.
  std::atomic_size_t _task_runned; // count of runned tasks.
};
}
}
}
