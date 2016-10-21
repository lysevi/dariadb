#include <libdariadb/storage/memstorage.h>
#include <memory>
#include <unordered_map>
#include <stx/btree_map.h>
#include <array>
using namespace dariadb;
using namespace dariadb::storage;

/**
Map:
  QueryId -> TimeTrack{ from,to,step MemChunkList[MemChunk{data}]}
*/

struct MemChunk {
    static const size_t MAX_MEM_CHUNK_SIZE = 512;
    std::array<Meas, MAX_MEM_CHUNK_SIZE> values;
};

using MemChunk_Ptr = std::shared_ptr<MemChunk>;
using MemChunkList = std::list<MemChunk_Ptr>;

struct TimeTrack {
    TimeTrack(const Time _step, const Id _meas_id, const Time _from, const Time _to) {
        step = _step;
        meas_id = _meas_id;
        from = _from;
        to = _to;
    }

    Id   meas_id;
    Time step;
    Time from;
    Time to;
    MemChunkList chunks;
};

using TimeTrack_ptr = std::shared_ptr<TimeTrack>;
using Id2Track = stx::btree_map<Id, TimeTrack_ptr>;

struct MemStorage::Private : public IMeasStorage {
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
  append_result append(const MeasArray & values) {
      return append_result();
  }
  append_result append(const Meas &value) override{
      return append_result();
  }
  void flush() override{

  }

  void foreach(const QueryTimePoint &q, IReaderClb *clbk) override{

  }
  MeasList readInterval(const QueryInterval &q) override{
      return MeasList{};
  }

  Id2Track _id2steps;
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

append_result dariadb::storage::MemStorage::append(const Meas &value){
    return _impl->append(value);
}

void dariadb::storage::MemStorage::flush(){
    _impl->flush();
}
