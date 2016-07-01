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
  std::unique_lock<std::mutex> lg(_queue_mutex);
  logger("tp post begin 1");
  TaskResult_Ptr res = std::make_shared<TaskResult>();
  logger("tp post begin 2");
  AsyncTask inner_task = [=](const ThreadInfo &ti) {
    try {
      task(ti);
      res->unlock();
    } catch (...) {
      res->unlock();
      throw;
    }
  };
logger("tp post begin 3");
  _in_queue.push_back(inner_task);
  logger("tp post begin 4");
  _condition.notify_one();
  logger("tp post begin 5");
  return res;
}

void ThreadPool::stop() {
    {
        std::unique_lock<std::mutex> lock(_queue_mutex);
        _stop_flag = true;
    }
    _condition.notify_all();
    for(std::thread &worker: _threads)
        worker.join();
    _is_stoped = true;
}

void ThreadPool::flush() {
  logger("TP::flush 1");
  while(true)  {
      if(_in_queue.empty()){
          break;
      }
  }
  logger("TP::flush 2");
}

void ThreadPool::_thread_func(size_t num) {
  ThreadInfo ti{};
  ti.kind = _params.kind;
  ti.thread_number = num;

  while (!_stop_flag) {
      AsyncTask task;

      {
          std::unique_lock<std::mutex> lock(_queue_mutex);
          this->_condition.wait(lock,
                               [this]{ return this->_stop_flag || !this->_in_queue.empty(); });
          if(this->_stop_flag)
              return;
          task = std::move(this->_in_queue.front());
          this->_in_queue.pop_front();
      }
      task(ti);
  }
  logger("thread #"<<num<<" stoped ");
}
