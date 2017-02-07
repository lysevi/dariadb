#pragma once

#include <extern/stx-btree/include/stx/btree_map.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/storage/memstorage/memchunk.h>
#include <memory>

namespace dariadb {
namespace storage {

class MemoryChunkContainer {
public:
  virtual void addChunk(MemChunk_Ptr &c) = 0;
  virtual void freeChunk(MemChunk_Ptr &c) = 0;
  virtual ~MemoryChunkContainer() {}
};

struct TimeTrack;
using TimeTrack_ptr = std::shared_ptr<TimeTrack>;
using Id2Track = std::unordered_map<Id, TimeTrack_ptr>;

struct TimeTrack : public IMeasStorage,
                   public std::enable_shared_from_this<TimeTrack> {
  TimeTrack(MemoryChunkContainer *mcc, const Time step, Id meas_id,
            MemChunkAllocator *allocator);
  ~TimeTrack();
  void updateMinMax(const Meas &value);
  virtual Status append(const Meas &value) override;
  void append_to_past(const Meas &value);
  void flush() override;
  Time minTime() override;
  Time maxTime() override;
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  Id2Cursor intervalReader(const QueryInterval &q)override;
  Statistic stat(const Id id, Time from, Time to)override;
  void foreach (const QueryInterval &q, IReadCallback * clbk) override;
  Id2Meas readTimePoint(const QueryTimePoint &q) override;
  virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;

  void rm_chunk(MemChunk *c);
  void rereadMinMax();
  bool create_new_chunk(const Meas &value);

  MemChunk_Ptr get_target_to_replace_from_index(const Time t);
  
  MemChunkAllocator *_allocator;
  Id _meas_id;
  MeasMinMax _min_max;
  Time _max_sync_time;
  Time _step;
  MemChunk_Ptr _cur_chunk;
  utils::async::Locker _locker;
  //stx::btree_map<Time, MemChunk_Ptr> _index;
  std::map<Time, MemChunk_Ptr> _index;
  MemoryChunkContainer *_mcc;
  bool is_locked_to_drop;
};
}
}