#pragma once

#include <libdariadb/st_exports.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/interfaces/ichunkcontainer.h>
#include <libdariadb/meas.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/settings.h>
#include <memory>

namespace dariadb {
namespace storage {

struct MemChunkAllocator {
	struct AllocatedData {
		ChunkHeader *header;
		uint8_t *buffer;
		size_t position;
		AllocatedData(ChunkHeader *h, uint8_t *buf, size_t pos) {
			header = h;
			buffer=buf;
			position = pos;
		}
		AllocatedData() {
			header = nullptr;
			buffer = nullptr;
			position = std::numeric_limits<size_t>::max();
		}
	};
  
  const AllocatedData EMPTY = AllocatedData(nullptr, nullptr, std::numeric_limits<size_t>::max());

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
  EXPORT AllocatedData allocate();
  EXPORT void free(const AllocatedData &d);
};

class MemStorage : public IMeasStorage {
public:
  struct Description {
	  size_t allocated;
      size_t allocator_capacity;
  };
public:
  EXPORT MemStorage(const storage::Settings_ptr &s);
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
  EXPORT void setDownLevel(IChunkContainer*_down);
  EXPORT void stop();
  EXPORT void lock_drop();
  EXPORT void unlock_drop();
private:
  struct Private;
  std::unique_ptr<Private> _impl;
};
using MemStorage_ptr = std::shared_ptr<MemStorage>;
}
}
