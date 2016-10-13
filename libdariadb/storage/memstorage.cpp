#include <libdariadb/storage/memstorage.h>
#include <memory>
#include <unordered_map>
#include <stx/btree_map.h>
using namespace dariadb;
using namespace dariadb::storage;
/**
Map:
  id -> 
       hash:
	       step-> Chunk{from,to,step,data_array}
*/

struct MemChunk {
	Time from;
	Time to;
	Time step;
	MeasArray values;
};
using MemChunk_Ptr = std::shared_ptr<MemChunk>;
using Step2Chunk = std::unordered_map<Time, MemChunk_Ptr>;
using Id2Step = stx::btree_map<Id, Step2Chunk>;

struct MemStorage::Private : public IMeasSource {
  Private(const MemStorage::Params &p) {}

  
  virtual Time minTime() override { return Time(); }
  virtual Time maxTime() override { return Time(); }
  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) override {
    return false;
  }
  virtual void foreach (const QueryInterval &q, IReaderClb * clbk) override {}
  virtual Id2Meas readTimePoint(const QueryTimePoint &q) override { return Id2Meas(); }
  virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override {
    return Id2Meas();
  }
  append_result append(const Time&step, const MeasArray & values) { 
	  return append_result(); 
  }

  Id2Step _id2steps;
};

MemStorage::MemStorage(const MemStorage::Params &p) : _impl(new MemStorage::Private(p)) {}

MemStorage::~MemStorage() {
  _impl = nullptr;
}

Time dariadb::storage::MemStorage::minTime() {
	return _impl->minTime();
}

Time dariadb::storage::MemStorage::maxTime() {
	return _impl->maxTime();
}

bool dariadb::storage::MemStorage::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                                            dariadb::Time *maxResult) {
  return _impl->minMaxTime(id,minResult, maxResult);
}

void dariadb::storage::MemStorage::foreach (const QueryInterval &q, IReaderClb * clbk) {
	_impl->foreach(q, clbk);
}

Id2Meas dariadb::storage::MemStorage::readTimePoint(const QueryTimePoint &q) {
	return _impl->readTimePoint(q);
}

Id2Meas dariadb::storage::MemStorage::currentValue(const IdArray &ids, const Flag &flag) {
	return _impl->currentValue(ids, flag);
}

append_result dariadb::storage::MemStorage::append(const Time&step, const MeasArray & values) {
	return _impl->append(step, values);
}
