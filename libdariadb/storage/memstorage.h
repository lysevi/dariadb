#pragma once

#include <libdariadb/st_exports.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/meas.h>
#include <libdariadb/storage/chunk.h>
#include <memory>

namespace dariadb {
namespace storage {

struct MemChunkAllocator {
  // header,buffer, position in allocator;
  typedef std::tuple<ChunkHeader *, uint8_t *, size_t> allocated_data;
  const allocated_data EMPTY = allocated_data(nullptr, nullptr, std::numeric_limits<size_t>::max());

  size_t _maxSize;
  size_t _bufferSize;
  size_t _capacity;
  size_t _allocated;
  std::vector<ChunkHeader> _headers;
  uint8_t *_buffers;
  std::list<size_t> _free_list;
  std::mutex _locker;

  EXPORT MemChunkAllocator(size_t maxSize, size_t bufferSize);
  EXPORT ~MemChunkAllocator();
  EXPORT allocated_data allocate();
  EXPORT void free(const allocated_data &d);
};

class MemStorage : public IMeasStorage {
public:
  struct Params {
	  static const size_t MAXIMUM_MEMORY_USAGE = 10 * 1024 * 1024; //10 mb
	  size_t max_size; //in bytes;
	  size_t chunk_size; //in bytes;
	  size_t id_count;//for pre-alloc table.
	  Params() {
		  id_count = 0;
		  max_size = MAXIMUM_MEMORY_USAGE;
		  chunk_size = 512;
	  }
  };
  //TODO rename to Description
  struct Description {
	  size_t allocated;
      size_t allocator_capacity;
  };
public:
  EXPORT MemStorage(const Params &p);
  EXPORT ~MemStorage();
  EXPORT Description description()const;

  // Inherited via IMeasStorage
  EXPORT virtual Time minTime() override;
  EXPORT virtual Time maxTime() override;
  EXPORT virtual bool minMaxTime(dariadb::Id id,
                                             dariadb::Time *minResult,
                                             dariadb::Time *maxResult) override;
  EXPORT virtual void foreach (const QueryInterval &q,
                                           IReaderClb * clbk) override;
  EXPORT virtual Id2Meas readTimePoint(const QueryTimePoint &q) override;
  EXPORT virtual Id2Meas currentValue(const IdArray &ids,
                                                  const Flag &flag) override;
  using IMeasStorage::append;
  EXPORT append_result append(const Meas &value) override;
  EXPORT void flush() override;
private:
  struct Private;
  std::unique_ptr<Private> _impl;
};
}
}
