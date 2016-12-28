#include <libdariadb/utils/async/thread_pool.h>
#include <libdariadb/utils/logger.h>
#include <cassert>

using namespace dariadb::utils;
using namespace dariadb::utils::async;

AsyncTaskWrap::AsyncTaskWrap(AsyncTask &t, const std::string &_function, const std::string &file, int line){
	_task = t;
	_parent_function = _function;
	_code_file = file;
	_code_line = line;
	_result = std::make_shared<TaskResult>();
}

bool AsyncTaskWrap::call(const ThreadInfo &ti) {
  _tinfo.kind = ti.kind;
  _tinfo.thread_number = ti.thread_number;

  if (!worker()) {
    _result->unlock();
    return false;
  }
  ///need recall.
  return true;
}

///return true if need recall.
bool AsyncTaskWrap::worker() {
  try {
    return _task(this->_tinfo);
  } catch (std::exception &ex) {
    logger_fatal("engine: *** async task exception:", _parent_function, " file:",
                 _code_file, " line:", _code_line);
    logger_fatal("engine: *** what:", ex.what());
    throw;
  }
}

TaskResult_Ptr AsyncTaskWrap::result()const {
	return _result;
}
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

TaskResult_Ptr ThreadPool::post(const AsyncTaskWrap_Ptr &task) {
  if (this->_is_stoped) {
    return nullptr;
  }
  pushTaskToQueue(task);
  return task->result();
}


void ThreadPool::stop() {
  {
    std::unique_lock<std::shared_mutex> lock(_queue_mutex);
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

void ThreadPool::pushTaskToQueue(const AsyncTaskWrap_Ptr&at) {
	{
		std::unique_lock<std::shared_mutex> lock(_queue_mutex);
		_in_queue.push_back(at);
	}
	_condition.notify_all();
}

void ThreadPool::_thread_func(size_t num) {
  ThreadInfo ti{};
  ti.kind = _params.kind;
  ti.thread_number = num;

  while (!_stop_flag) {
	  std::shared_ptr<AsyncTaskWrap> task;

    {
      std::unique_lock<std::shared_mutex> lock(_queue_mutex);
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
    
	//if queue is empty and task is coroutine, it will be run in cycle.
	while (true) {
		auto need_continue = task->call(ti);
		if (!need_continue) {
			break;
		}
		_queue_mutex.lock_shared();
		if (!_in_queue.empty() || this->_stop_flag) {
			_queue_mutex.unlock_shared();
			pushTaskToQueue(task);
			break;
		}
		_queue_mutex.unlock_shared();
	}
	--_task_runned;
    
  }
}
