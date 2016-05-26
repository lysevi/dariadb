#pragma once

#include <chrono>
#include <thread>

namespace dariadb {
namespace utils {

// look usage example in utils_test.cpp
class PeriodWorker {
public:
  PeriodWorker(const std::chrono::milliseconds sleep_time);
  virtual ~PeriodWorker();
  virtual void call() = 0;

  void start_worker();

  /// whait, while all works done and stop thread.
  void stop_worker();
  bool stoped() const { return m_stop_flag; }

protected:
  void _thread_func();

private:
  std::thread m_thread;
  bool m_stop_flag, m_thread_work;
  std::chrono::milliseconds _sleep_time;
};
}
}
