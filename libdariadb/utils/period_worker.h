#pragma once

#include <libdariadb/dariadb_st_exports.h>
#include <chrono>
#include <thread>

namespace dariadb {
namespace utils {

// look usage example in utils_test.cpp
class PeriodWorker {
public:
  DARIADB_ST_EXPORTS PeriodWorker(const std::chrono::milliseconds sleep_time);
  DARIADB_ST_EXPORTS virtual ~PeriodWorker();
  virtual void period_call() = 0;

  DARIADB_ST_EXPORTS void period_worker_start();

  /// whait, while all works done and stop thread.
  DARIADB_ST_EXPORTS void period_worker_stop();
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
