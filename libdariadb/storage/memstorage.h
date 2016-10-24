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
  const allocated_data EMPTY =
      allocated_data(nullptr, nullptr, std::numeric_limits<size_t>::max());

  size_t _maxSize;
  size_t _bufferSize;
  size_t _total_count;
  std::vector<ChunkHeader> _headers;
  uint8_t *_buffers;
  std::vector<uint8_t> _free_list;
  size_t allocated;
  utils::Locker _locker;

  EXPORT MemChunkAllocator(size_t maxSize, size_t bufferSize);
  EXPORT ~MemChunkAllocator();
  EXPORT allocated_data allocate();
  EXPORT void free(const allocated_data &d);
};

class MemStorage : public IMeasStorage {
public:
  struct Params {
	  static const size_t MAXIMUM_MEMORY_USAGE = 10 * 1024 * 1024; //one mb
	  size_t maxSize; //in bytes;
	  size_t id_count;//for pre-alloc table.
	  Params() {
		  id_count = 0;
		  maxSize = MAXIMUM_MEMORY_USAGE;
	  }
  };

public:
  EXPORT MemStorage(const Params &p);
  EXPORT ~MemStorage();

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
