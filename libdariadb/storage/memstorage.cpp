#include "memstorage.h"
#include "../utils/locker.h"
//#include "../utils/utils.h"
#include "chunk.h"
#include "cursor.h"
#include <algorithm>
#include <assert.h>
#include <unordered_map>


using namespace dariadb;
using namespace dariadb::compression;
using namespace dariadb::storage;

class MemstorageCursor : public Cursor {
public:
  ChunksList _chunks;
  ChunksList::iterator it;
  MemstorageCursor(ChunksList &chunks) : _chunks(chunks.begin(), chunks.end()) {
    this->reset_pos();
  }
  ~MemstorageCursor() { _chunks.clear(); }

  bool is_end() const override { return it == _chunks.end(); }

  void readNext(Cursor::Callback *cbk) override {
    if (!is_end()) {
      cbk->call(*it);
      ++it;
    } else {
      Chunk_Ptr empty;
      cbk->call(empty);
    }
  }

  void reset_pos() override { it = _chunks.begin(); }
};

class MemoryStorage::Private
/* : protected dariadb::utils::AsyncWorker<Chunk_Ptr>*/ {
public:
  Private() { _cc = nullptr; }

  ~Private() {}

  dariadb::storage::Reader_ptr readInterval(Time from, Time to) {
    NOT_IMPLEMENTED;
    //return nullptr;
  }
  dariadb::storage::Reader_ptr readInTimePoint(Time time_point) {
    NOT_IMPLEMENTED;
    //return nullptr;
  }
  dariadb::storage::Reader_ptr readInterval(const QueryInterval &q) {
    NOT_IMPLEMENTED;
    //return nullptr;
  }
  dariadb::storage::Reader_ptr readInTimePoint(const QueryTimePoint &q) {
    NOT_IMPLEMENTED;
    //return nullptr;
  }

  dariadb::storage::Reader_ptr currentValue(const IdArray &ids,
                                            const Flag &flag) {
    NOT_IMPLEMENTED;
    //return nullptr;
  }

  Time minTime() {
    NOT_IMPLEMENTED;
    //return 0;
  }
  Time maxTime() {
    NOT_IMPLEMENTED;
    //return 0;
  }

  void set_chunkSource(ChunkContainer *cw) { _cc = cw; }

protected:
  // size_t _size;
  ChunkContainer *_cc;
};

MemoryStorage::MemoryStorage() : _Impl(new MemoryStorage::Private{}) {}

MemoryStorage::~MemoryStorage() {}

Time MemoryStorage::minTime() {
  return _Impl->minTime();
}

Time MemoryStorage::maxTime() {
  return _Impl->maxTime();
}

Reader_ptr MemoryStorage::currentValue(const IdArray &ids, const Flag &flag) {
  return _Impl->currentValue(ids, flag);
}

void MemoryStorage::set_chunkSource(ChunkContainer *cw) {
  _Impl->set_chunkSource(cw);
}

Reader_ptr MemoryStorage::readInterval(const QueryInterval &q) {
  return _Impl->readInterval(q);
}

Reader_ptr MemoryStorage::readInTimePoint(const QueryTimePoint &q) {
  return _Impl->readInTimePoint(q);
}
