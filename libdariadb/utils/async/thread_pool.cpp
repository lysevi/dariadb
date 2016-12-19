#include <libdariadb/utils/async/thread_pool.h>
#include <libdariadb/utils/logger.h>
#include <cassert>

using namespace dariadb::utils;
using namespace dariadb::utils::async;

AsyncTaskWrap::AsyncTaskWrap(AsyncTask &t, const std::string &_function, const std::string &file, int line)
	:_coro(std::bind(&AsyncTaskWrap::worker, this, std::placeholders::_1)) {
	_task = t;
	_parent_function = _function;
	_code_file = file;
	_code_line = line;
	_result = std::make_shared<TaskResult>();
	_tinfo.coro_yield = nullptr;
	/*auto f = std::bind(&AsyncTaskWrap::call, this, std::placeholders::_1);
	_coro.reset(new Coroutine(f));*/
}

bool AsyncTaskWrap::call(const ThreadInfo &ti) {
  _tinfo.kind = ti.kind;
  _tinfo.thread_number = ti.thread_number;

  if (!_coro()) {
    _result->unlock();
    return false;
  }
  return true;
}

void AsyncTaskWrap::worker(Yield &y) {
  try {
    this->_tinfo.coro_yield = &y;
    _task(this->_tinfo);
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

void ThreadPool::pushTaskToQueue(const AsyncTaskWrap_Ptr&at) {
	{
		std::unique_lock<std::mutex> lock(_queue_mutex);
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
    auto need_continue=task->call(ti);
	if (need_continue) {
		pushTaskToQueue(task);
	}
	--_task_runned;
    // logger("run: "<<task.parent_function<<" file:"<<task.code_file <<" ok");
  }
}
