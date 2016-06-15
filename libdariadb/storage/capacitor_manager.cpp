#include "capacitor_manager.h"
#include "../utils/exception.h"
#include <cassert>

using namespace dariadb::storage;

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
}

CapacitorManager * dariadb::storage::CapacitorManager::instance(){
	return CapacitorManager::_instance;
}
