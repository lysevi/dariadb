#pragma once

#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/storage/memstorage/memchunk.h>
#include <extern/stx-btree/include/stx/btree_map.h>

namespace dariadb {
namespace storage {

class MemoryChunkContainer {
public:
  virtual void addChunk(MemChunk_Ptr &c) = 0;
  virtual ~MemoryChunkContainer() {}
};

struct TimeTrack : public IMeasStorage {
  TimeTrack(MemoryChunkContainer *mcc, const Time step, Id meas_id,
            MemChunkAllocator *allocator);
  ~TimeTrack();
  void updateMinMax(const Meas &value);
  virtual Status append(const Meas &value) override;
  void flush() override;
  Time minTime() override;
  Time maxTime() override;
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  void foreach (const QueryInterval &q, IReaderClb * clbk) override;
  void foreach_interval_call(const MemChunk_Ptr &c, const QueryInterval &q,
                             IReaderClb *clbk);
  Id2Meas readTimePoint(const QueryTimePoint &q) override;
  virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;

  void rm_chunk(MemChunk *c);
  void rereadMinMax();
  bool create_new_chunk(const Meas &value);

  MemChunkAllocator *_allocator;
  Id _meas_id;
  MeasMinMax _min_max;
  Time _max_sync_time;
  Time _step;
  MemChunk_Ptr _cur_chunk;
  utils::async::Locker _locker;
  stx::btree_map<Time, MemChunk_Ptr> _index;
  MemoryChunkContainer *_mcc;
};

using TimeTrack_ptr = std::shared_ptr<TimeTrack>;
using Id2Track = std::unordered_map<Id, TimeTrack_ptr>;
}
}