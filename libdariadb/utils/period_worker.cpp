#include "period_worker.h"
#include <assert.h>

using namespace dariadb::utils;

PeriodWorker::PeriodWorker(const std::chrono::milliseconds sleep_time) {
  _sleep_time = sleep_time;
}

PeriodWorker::~PeriodWorker() {
  if (m_thread_work) {
    this->stop_worker();
  }
}

void PeriodWorker::start_worker() {
  m_stop_flag = false;
  m_thread = std::thread(&PeriodWorker::_thread_func, this);
  assert(m_thread.joinable());
}

/// whait, while all works done and stop thread.
void PeriodWorker::stop_worker() {
  m_stop_flag = true;
  m_thread.join();
}

void PeriodWorker::_thread_func() {
  m_thread_work = true;
  while (!m_stop_flag) {
    std::this_thread::sleep_for(_sleep_time);
    this->call();
  }
  m_thread_work = false;
}
