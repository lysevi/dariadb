#pragma once

#include <libdariadb/st_exports.h>
#include <libdariadb/utils/async/locker.h>
#include <libdariadb/utils/utils.h>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <shared_mutex>
#include <thread>
#include <vector>
namespace dariadb {
namespace utils {
namespace async {

using ThreadKind = uint16_t;
// TODO rename to THREAD_KINDS
enum class THREAD_KINDS : ThreadKind { DISK_IO = 1, COMMON };

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

/// return true if need recall.
using AsyncTask = std::function<bool(const ThreadInfo &)>;

class AsyncTaskWrap {
public:
  EXPORT AsyncTaskWrap(AsyncTask &t, const std::string &_function,
                       const std::string &file, int line);
  EXPORT bool call(const ThreadInfo &ti);
  EXPORT TaskResult_Ptr result() const;

private:
  /// return true if need recall.
  bool worker();

private:
  ThreadInfo _tinfo;
  TaskResult_Ptr _result;
  AsyncTask _task;
  std::string _parent_function;
  std::string _code_file;
  int _code_line;
};
using AsyncTaskWrap_Ptr = std::shared_ptr<AsyncTaskWrap>;
#define AT(task)                                                                         \
  std::make_shared<AsyncTaskWrap>(task, std::string(__FUNCTION__),                       \
                                  std::string(__FILE__), __LINE__)

using TaskQueue = std::deque<AsyncTaskWrap_Ptr>;

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
  EXPORT ThreadPool(const Params &p);
  EXPORT ~ThreadPool();
  size_t threads_count() const { return _params.threads_count; }
  ThreadKind kind() const { return _params.kind; }

  bool isStoped() const { return _is_stoped; }

  EXPORT TaskResult_Ptr post(const AsyncTaskWrap_Ptr &task);
  EXPORT void flush();
  EXPORT void stop();

  size_t active_works() {
    std::shared_lock<std::shared_mutex> lg(_queue_mutex);
    size_t res = _in_queue.size();
    return res + (_task_runned);
  }

protected:
  void _thread_func(size_t num);
  void pushTaskToQueue(const AsyncTaskWrap_Ptr &at);

protected:
  Params _params;
  std::vector<std::thread> _threads;
  TaskQueue _in_queue;
  std::shared_mutex _queue_mutex;
  std::condition_variable_any _condition;
  bool _stop_flag;                 // true - pool under stop.
  bool _is_stoped;                 // true - already stopped.
  std::atomic_size_t _task_runned; // count of runned tasks.
};
}
}
}
