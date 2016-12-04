#include <libdariadb/utils/thread_pool.h>
#include <libdariadb/utils/logger.h>
#include <cassert>

using namespace dariadb::utils;
using namespace dariadb::utils::async;

ThreadPool::ThreadPool(const Params &p) : _params(p) {
  assert(_params.threads_count > 0);
  _stop_flag = false;
  _is_stoped = false;
  _task_runned = size_t(0);
  _threads.resize(_params.threads_count);
  for (size_t i = 0; i < _params.threads_count; ++i) {
    _threads[i] = std::thread{&ThreadPool::_thread_func, this, i};
  }
}

ThreadPool::~ThreadPool() {
  if (!_is_stoped) {
    stop();
  }
}

TaskResult_Ptr ThreadPool::post(const AsyncTaskWrap &task) {
  std::unique_lock<std::mutex> lg(_queue_mutex);
  if (this->_is_stoped) {
    return nullptr;
  }
  TaskResult_Ptr res = std::make_shared<TaskResult>();
  AsyncTask inner_task = [=](const ThreadInfo &ti) {
    try {
      task.task(ti);
      res->unlock();
    } catch (std::exception &ex) {
      logger_fatal("engine: *** async task exception:", task.parent_function, " file:",
                   task.code_file, " line:", task.code_line);
      logger_fatal("engine: *** what:", ex.what());
      res->unlock();
      throw;
    }
  };
  _in_queue.push_back(
      AsyncTaskWrap(inner_task, task.parent_function, task.code_file, task.code_line));
  _condition.notify_all();
  return res;
}

void ThreadPool::stop() {
  {
    std::unique_lock<std::mutex> lock(_queue_mutex);
    _stop_flag = true;
  }
  _condition.notify_all();
  for (std::thread &worker : _threads)
    worker.join();
  _is_stoped = true;
}

void ThreadPool::flush() {
  while (true) {
    _condition.notify_one();
    if (_in_queue.empty() && (_task_runned.load() == size_t(0))) {
      break;
	}
	else {
		std::this_thread::yield();
	}	
  }
}

void ThreadPool::_thread_func(size_t num) {
  ThreadInfo ti{};
  ti.kind = _params.kind;
  ti.thread_number = num;

  while (!_stop_flag) {
    AsyncTaskWrap task;

    {
      std::unique_lock<std::mutex> lock(_queue_mutex);
      this->_condition.wait(
          lock, [this] { 
		  return this->_stop_flag || !this->_in_queue.empty(); 
	  });
	  if (this->_stop_flag) {
		  return;
	  }
      _task_runned++;
      task = std::move(this->_in_queue.front());
      this->_in_queue.pop_front();
    }
    // logger("run: "<<task.parent_function<<" file:"<<task.code_file);
    task.task(ti);
    --_task_runned;
    // logger("run: "<<task.parent_function<<" file:"<<task.code_file <<" ok");
  }
}
