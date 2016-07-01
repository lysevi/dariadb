#pragma once

#include "locker.h"
#include "utils.h"
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <deque>
#include <thread>
#include <vector>

namespace dariadb {
namespace utils {
namespace async {

using ThreadKind = uint16_t;

struct ThreadInfo {
  ThreadKind kind;
  size_t thread_number;
};

using AsyncTask = std::function<void(const ThreadInfo &)>;
using TaskQueue = std::deque<AsyncTask>;

class ThreadPool : public utils::NonCopy {
public:
  ThreadPool(size_t threads_count, ThreadKind kind);
  ~ThreadPool();
  size_t threads_count() const { return _threads_count; }
  bool is_stoped() const { return _is_stoped; }

  void post(const AsyncTask task);
  void flush();
  void stop();

protected:
  void _thread_func(size_t num);

protected:
  ThreadKind _kind;
  size_t _threads_count;
  std::vector<std::thread> _threads;
  mutable Locker _locker;
  TaskQueue _in_queue;
  std::condition_variable _data_cond;
  bool _stop_flag, _is_stoped;
};
}
}
}