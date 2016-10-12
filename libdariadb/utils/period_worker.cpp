#include <libdariadb/utils/period_worker.h>
#include <assert.h>

using namespace dariadb::utils;

PeriodWorker::PeriodWorker(const std::chrono::milliseconds sleep_time) {
  _sleep_time = sleep_time;
  m_thread_work = false;
}

PeriodWorker::~PeriodWorker() {
  if (m_thread_work) {
    this->period_worker_stop();
  }
}

void PeriodWorker::period_worker_start() {
  m_stop_flag = false;
  m_thread = std::thread(&PeriodWorker::_thread_func, this);
  assert(m_thread.joinable());
}

/// whait, while all works done and stop thread.
void PeriodWorker::period_worker_stop() {
  m_stop_flag = true;
  m_thread.join();
}

void PeriodWorker::_thread_func() {
  m_thread_work = true;
  while (!m_stop_flag) {
    std::this_thread::sleep_for(_sleep_time);
    this->period_call();
  }
  m_thread_work = false;
}
