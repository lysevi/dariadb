#include "union_storage.h"

using namespace dariadb;
using namespace dariadb::storage;

class UnionStorage::Private {
public:
	Private(const std::string &path, STORAGE_MODE mode, size_t chunk_per_storage, size_t chunk_size) {
	}
	Time minTime() {
		return 0;
	}

	Time maxTime() {
		return 0;
	}
	
	append_result append(const Meas::PMeas begin, const size_t size) {
		return append_result(0, 0);
	}
	
	append_result append(const Meas &value) {
		return append_result(0, 0);
	}
	
	Reader_ptr readInterval(const IdArray &ids, Flag flag, Time from, Time to) {
		return nullptr;
	}
	Reader_ptr readInTimePoint(const IdArray &ids, Flag flag, Time time_point) {
		return nullptr;
	}
	
	void subscribe(const IdArray&ids, const Flag& flag, const ReaderClb_ptr &clbk) {
		
	}
	
	Reader_ptr currentValue(const IdArray&ids, const Flag& flag) {
		return nullptr;
	}
};

UnionStorage::UnionStorage(const std::string &path, STORAGE_MODE mode, size_t chunk_per_storage, size_t chunk_size):
	_impl{new UnionStorage::Private(path,mode,chunk_per_storage,chunk_size)}
{
}

Time UnionStorage::minTime(){
	return _impl->minTime();
}

Time UnionStorage::maxTime(){
	return _impl->maxTime();
}

append_result UnionStorage::append(const Meas::PMeas begin, const size_t size){
	return _impl->append(begin, size);
}

append_result UnionStorage::append(const Meas &value){
	return _impl->append(value);
}

Reader_ptr UnionStorage::readInterval(const IdArray &ids, Flag flag, Time from, Time to){
	return _impl->readInterval(ids, flag, from, to);
}

Reader_ptr UnionStorage::readInTimePoint(const IdArray &ids, Flag flag, Time time_point){
	return _impl->readInTimePoint(ids, flag, time_point);
}

void UnionStorage::subscribe(const IdArray&ids, const Flag& flag, const ReaderClb_ptr &clbk){
	_impl->subscribe(ids, flag, clbk);
}

Reader_ptr UnionStorage::currentValue(const IdArray&ids, const Flag& flag){
	return _impl->currentValue(ids, flag);
}