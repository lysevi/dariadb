#include "thread_pool.h"
#include "logger.h"
#include <cassert>

using namespace dariadb::utils;
using namespace dariadb::utils::async;

ThreadPool::ThreadPool(const Params &p) : _params(p) {
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
  std::unique_lock<std::mutex> lg(_locker);
  logger("tp post begin");
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
    if(_is_stoped){
        return;
    }
  this->flush();
  _stop_flag = true;
  _data_cond.notify_all();
  for (auto &t : _threads) {
      _data_cond.notify_all();
    t.join();
  }
  _is_stoped = true;
}

void ThreadPool::flush() {
  if (_in_queue.empty()) {
    return;
  }
  std::mutex local_lock;
  std::unique_lock<std::mutex> lk(local_lock);
  logger("TP::flush 1");
  _flush_cond.wait(lk, [&] { return _in_queue.empty(); });
  logger("TP::flush 2");
}

void ThreadPool::_thread_func(size_t num) {
  std::mutex local_lock;
  ThreadInfo ti{};
  ti.kind = _params.kind;
  ti.thread_number = num;

  while (!_stop_flag) {
    std::unique_lock<std::mutex> lk(local_lock);
    _data_cond.wait(lk, [&] { return !_in_queue.empty() || _stop_flag; });
    if(_stop_flag){
        break;
    }

    if (!_in_queue.empty()) {
        AsyncTask task;
        {
            std::lock_guard<std::mutex> lg(_locker);
            if(_in_queue.empty()){
                continue;
            }
            task = _in_queue.front();
            _in_queue.pop_front();
        }

        try {
            task(ti);
        } catch (std::exception &ex) {
            logger_fatal("thread pool kind=" << _params.kind << " #" << num
                         << " task error: " << ex.what());
        }
    }
    _flush_cond.notify_one();
  }
  logger("thread #"<<num<<" stoped");
}
