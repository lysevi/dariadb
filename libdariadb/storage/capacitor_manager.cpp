#include "capacitor_manager.h"
#include "../utils/exception.h"
#include <cassert>

using namespace dariadb::storage;
using namespace dariadb;

CapacitorManager *CapacitorManager::_instance = nullptr;

CapacitorManager::~CapacitorManager(){
}

CapacitorManager::CapacitorManager(const Params & param):_params(param){
}

void CapacitorManager::start(const Params & param){
	if (CapacitorManager::_instance == nullptr) {
		CapacitorManager::_instance = new CapacitorManager(param);
	}
	else {
		throw MAKE_EXCEPTION("CapacitorManager::start started twice.");
	}
}

void CapacitorManager::stop(){
	delete CapacitorManager::_instance;
	CapacitorManager::_instance = nullptr;
}

CapacitorManager * dariadb::storage::CapacitorManager::instance(){
	return CapacitorManager::_instance;
}

dariadb::Time CapacitorManager::minTime(){
	return Time();
}

dariadb::Time CapacitorManager::maxTime()
{
	return Time();
}

bool CapacitorManager::minMaxTime(dariadb::Id id, dariadb::Time * minResult, dariadb::Time * maxResult){
	return false;
}

Reader_ptr CapacitorManager::readInterval(const QueryInterval & q){
	return Reader_ptr();
}

Reader_ptr CapacitorManager::readInTimePoint(const QueryTimePoint & q){
	return Reader_ptr();
}

Reader_ptr CapacitorManager::currentValue(const IdArray & ids, const Flag & flag){
	return Reader_ptr();
}

dariadb::append_result CapacitorManager::append(const Meas & value){
	return append_result();
}

void CapacitorManager::flush(){
}

void CapacitorManager::subscribe(const IdArray & ids, const Flag & flag, const ReaderClb_ptr & clbk){
	NOT_IMPLEMENTED;
}
