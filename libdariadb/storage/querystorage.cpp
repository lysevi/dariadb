#include <libdariadb/storage/querystorage.h>
#include <libdariadb/utils/locker.h>
#include <memory>
#include <array>
#include <atomic>
#include <stx/btree_map.h>

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
using Id2Step = stx::btree_map<Id, TimeTrack_ptr>;

struct QueryStorage::Private {
  Private(const QueryStorage::Params &p):_next_query_id(size_t(0)){}

  Id begin(const Time step, const Id meas_id, const Time from, const Time to) {
	  std::lock_guard<utils::Locker> lg(_locker);

	  TimeTrack_ptr tptr{ new TimeTrack(step,meas_id, from, to) };
	  
	  auto cur_id= _next_query_id++;
	  _id2steps[cur_id] = tptr;
	  
	  return cur_id;
  }
  
  append_result append(const Time step, const Meas &value) { 
	  return append_result(); 
  }

  std::atomic_size_t _next_query_id;
  Id2Step _id2steps;
  utils::Locker _locker;
};

QueryStorage::QueryStorage(const QueryStorage::Params &p) : _impl(new QueryStorage::Private(p)) {}

QueryStorage::~QueryStorage() {
  _impl = nullptr;
}

Id QueryStorage::begin(const Time step, const Id meas_id, const Time from, const Time to) {
	return _impl->begin(step, meas_id, from, to);
}

append_result QueryStorage::append(const Time step, const Meas &value) {
	return _impl->append(step, value);
}
