#include "logger.h"
#include "thread_pool.h"
#include <cassert>

using namespace dariadb::utils;
using namespace dariadb::utils::async;

ThreadPool::ThreadPool(const Params&p)
    : _params(p) {
  assert(_params.threads_count > 0);
  _stop_flag = false;
  _is_stoped = false;

  _threads.resize(_params.threads_count);
  for (size_t i = 0; i < _params.threads_count; ++i) {
    _threads[i] = std::move(std::thread{&ThreadPool::_thread_func, this, i});
  }
}

ThreadPool::~ThreadPool() {
  if (!_is_stoped) {
    stop();
  }
}

TaskResult_Ptr ThreadPool::post(const AsyncTask task) {
  std::unique_lock<utils::Locker> lg(_locker);

  TaskResult_Ptr res = std::make_shared<TaskResult>();
  AsyncTask inner_task = [=](const ThreadInfo &ti) {
    try {
      task(ti);
      res->unlock();
    } catch (...) {
      res->unlock();
      throw;
    }
  };

  _in_queue.push_back(inner_task);
  _data_cond.notify_all();
  return res;
}

void ThreadPool::stop() {
	this->flush();
  _stop_flag = true;
  _data_cond.notify_all();
  for (auto &t : _threads) {
    t.join();
  }
  _is_stoped = true;
}

void ThreadPool::flush() {
  while (!this->_in_queue.empty()) {
    std::this_thread::yield();
  }
}

void ThreadPool::_thread_func(size_t num) {
  std::mutex local_lock;
  ThreadInfo ti{};
  ti.kind = _params.kind;
  ti.thread_number = num;
  while (!_stop_flag) {
    std::unique_lock<std::mutex> lk(local_lock);
    _data_cond.wait(lk, [&] { return !_in_queue.empty() || _stop_flag; });
    _locker.lock();
    if (!_in_queue.empty()) {
      TaskQueue local_queue{_in_queue.begin(), _in_queue.end()};
      _in_queue.clear();
      _locker.unlock();

      for (auto &task : local_queue) {
        try {
          task(ti);
        } catch (std::exception &ex) {
          logger_fatal("thread pool kind=" << _params.kind << " #" << num
                                           << " task error: " << ex.what());
        }
      }
    } else {
      _locker.unlock();
    }
  }
}