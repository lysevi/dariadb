#pragma once

#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/storage/memstorage/memchunk.h>
#include <libdariadb/utils/striped_map.h>
#include <libdariadb/utils/utils.h>
#include <extern/stx-btree/include/stx/btree_map.h>
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
// using Id2Track = std::unordered_map<Id, TimeTrack_ptr>;
using Id2Track = utils::stripped_map<Id, TimeTrack_ptr>;

struct TimeTrack final : public IMeasStorage,
                         public std::enable_shared_from_this<TimeTrack> {
  TimeTrack(MemoryChunkContainer *mcc, const Time step, Id meas_id,
            IMemoryAllocator_Ptr allocator);
  ~TimeTrack();
  void updateMinMax(const Meas &value);
  virtual Status append(const Meas &value) override;
  void append_to_past(const Meas &value);
  void flush() override;
  Time minTime() override;
  Time maxTime() override;
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  Id2Cursor intervalReader(const QueryInterval &q) override;
  Statistic stat(const Id id, Time from, Time to) override;
  void foreach (const QueryInterval &q, IReadCallback * clbk) override;
  Id2Meas readTimePoint(const QueryTimePoint &q) override;
  virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;

  Id2MinMax_Ptr loadMinMax() override { NOT_IMPLEMENTED; }

  void rereadMinMax();
  bool create_new_chunk(const Meas &value);

  size_t chunks_count() {
    std::lock_guard<std::mutex> lg(_locker);
    return _index.size();
  }

  std::vector<MemChunk_Ptr> drop_N(size_t n) {
    std::vector<MemChunk_Ptr> result;
    result.reserve(n);
    {
      std::lock_guard<std::mutex> lg(_locker);

      auto cnt = n;
      while (!_index.empty()) {
        auto front = _index.begin();
        result.push_back(front->second);
        _index.erase(front);
        --cnt;
        if (cnt == size_t(0)) {
          break;
        }
      }
    }
    rereadMinMax();
    return result;
  }

  size_t drop_Old(Time t) {
    std::lock_guard<std::mutex> lg(_locker);
    size_t erased = 0;
    while (!_index.empty()) {
      bool find_one = false;

      auto it = _index.begin();
      auto c = it->second;
      if (c->header->stat.maxTime < t) {
        find_one = true;
        _index.erase(it);
        erased++;
      }
      if (!find_one) {
        break;
      }
    }
    if (_cur_chunk->header->stat.maxTime < t) {
      ++erased;
      _cur_chunk = nullptr;
    }
    return erased;
  }

  MemChunk_Ptr get_target_to_replace_from_index(const Time t);

  IMemoryAllocator_Ptr _allocator;
  Id _meas_id;
  MeasMinMax _min_max;
  Time _max_sync_time;
  Time _step;
  MemChunk_Ptr _cur_chunk;
  std::mutex _locker;
  // stx::btree_map<Time, MemChunk_Ptr> _index;
  std::map<Time, MemChunk_Ptr> _index;
  MemoryChunkContainer *_mcc;
};
} // namespace storage
} // namespace dariadb