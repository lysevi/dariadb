#include "thread_manager.h"
#include "exception.h"

using namespace dariadb::utils::async;

ThreadManager* ThreadManager::_instance = nullptr;

void ThreadManager::start(const ThreadManager::Params &params) {
	if (_instance == nullptr) {
        logger("tm start");
		_instance = new ThreadManager(params);
	}
}

void ThreadManager::stop() {
    logger("tm stop begin");
	delete _instance;
	_instance = nullptr;
    logger("tm stoped");
}

ThreadManager* ThreadManager::instance() {
	return _instance;
}

ThreadManager::ThreadManager(const ThreadManager::Params &params):_params(params) {
	for (auto kv : _params.pools) {
		_pools[kv.kind] = std::make_shared<ThreadPool>(kv);
	}
    _stoped=false;
}

void ThreadManager::flush() {
    logger("tm flush begin");
	for (auto&kv : _pools) {
		kv.second->flush();
	}
    logger("tm flush end");
}
TaskResult_Ptr dariadb::utils::async::ThreadManager::post(const ThreadKind kind, const AsyncTask task){
	auto target = _pools.find(kind);
	if (target == _pools.end()) {
		throw MAKE_EXCEPTION("unknow kind.");
	}
	return target->second->post(task);
}

ThreadManager::~ThreadManager() {
    logger("tm ~ThreadManager begin");
    if(!_stoped){
        for (auto&kv : _pools) {
            kv.second->flush();
            kv.second->stop();
        }
        _pools.clear();
    }
    logger("tm ~ThreadManager end");
}
