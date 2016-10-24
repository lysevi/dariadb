#include <array>
#include <libdariadb/storage/memstorage.h>
#include <memory>
#include <stx/btree_map.h>
#include <tuple>
#include <unordered_map>

using namespace dariadb;
using namespace dariadb::storage;

MemChunkAllocator::MemChunkAllocator(size_t maxSize, size_t bufferSize) {
  _maxSize = maxSize;
  _bufferSize = bufferSize;
  allocated = size_t(0);
  size_t one_sz = sizeof(ChunkHeader) + bufferSize;
  _total_count = (int)(float(_maxSize) / one_sz);

  _headers.resize(_total_count);
  _buffers = new uint8_t[_total_count * _bufferSize];
  _free_list.resize(_total_count);
  std::fill(_free_list.begin(), _free_list.end(), uint8_t(0));
}

MemChunkAllocator::~MemChunkAllocator() {
  delete[] _buffers;
}

MemChunkAllocator::allocated_data MemChunkAllocator::allocate() {
  _locker.lock();
  for (size_t i = 0; i < _total_count; ++i) {
    if (_free_list[i] == uint8_t(0)) {
      _free_list[i] = 1;
      allocated++;
      _locker.unlock();
      return allocated_data(&_headers[i], &_buffers[i * _bufferSize], i);
    }
  }
  _locker.unlock();
  return EMPTY;
}

void MemChunkAllocator::free(const MemChunkAllocator::allocated_data &d) {
  auto header = std::get<0>(d);
  auto buffer = std::get<1>(d);
  auto position = std::get<2>(d);
  memset(header, 0, sizeof(ChunkHeader));
  memset(buffer, 0, _bufferSize);
  _locker.lock();
  allocated--;
  _free_list[position] = 0;
  _locker.unlock();
}

/**
Map:
  Meas.id -> TimeTrack{ MemChunkList[MemChunk{data}]}
*/

struct MemChunk : public ZippedChunk {
  ChunkHeader *index_ptr;
  uint8_t *buffer_ptr;
  MemChunkAllocator::allocated_data _a_data;
  MemChunk(ChunkHeader *index, uint8_t *buffer, size_t size, Meas first_m)
      : ZippedChunk(index, buffer, size, first_m) {
    index_ptr = index;
    buffer_ptr = buffer;
  }
  MemChunk(ChunkHeader *index, uint8_t *buffer) : ZippedChunk(index, buffer) {
    index_ptr = index;
    buffer_ptr = buffer;
  }
  ~MemChunk() {}
};

using MemChunk_Ptr = std::shared_ptr<MemChunk>;
using MemChunkList = std::list<MemChunk_Ptr>;

class TimeTrack : public IMeasStorage {
public:
  TimeTrack(const Time step) {
    _step = step;
    _minTime = _maxTime = Time(0);
  }
  ~TimeTrack() {}

  Time _minTime;
  Time _maxTime;
  Time _step;
  MemChunkList _chunks;
  utils::Locker _locker;

  Time minTime() override { return Time(); }

  Time maxTime() override { return Time(); }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override {
    return false;
  }

  void foreach (const QueryInterval &q, IReaderClb * clbk) override {}

  virtual Id2Meas readTimePoint(const QueryTimePoint &q) override { return Id2Meas(); }

  Id2Meas currentValue(const IdArray &ids, const Flag &flag) override {
    return Id2Meas();
  }
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
  append_result append(const MeasArray &values) { return append_result(); }
  append_result append(const Meas &value) override { return append_result(); }
  void flush() override {}

  void foreach (const QueryTimePoint &q, IReaderClb * clbk) override {}
  MeasList readInterval(const QueryInterval &q) override { return MeasList{}; }

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
  return _impl->minMaxTime(id, minResult, maxResult);
}

void dariadb::storage::MemStorage::foreach (const QueryInterval &q, IReaderClb * clbk) {
  _impl->foreach (q, clbk);
}

Id2Meas dariadb::storage::MemStorage::readTimePoint(const QueryTimePoint &q) {
  return _impl->readTimePoint(q);
}

Id2Meas dariadb::storage::MemStorage::currentValue(const IdArray &ids, const Flag &flag) {
  return _impl->currentValue(ids, flag);
}

append_result dariadb::storage::MemStorage::append(const Meas &value) {
  return _impl->append(value);
}

void dariadb::storage::MemStorage::flush() {
  _impl->flush();
}
