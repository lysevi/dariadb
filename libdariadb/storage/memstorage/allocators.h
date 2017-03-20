#pragma once

#include <libdariadb/st_exports.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/utils/async/locker.h>
#include <libdariadb/utils/utils.h>
#include <memory>

#include <boost/lockfree/queue.hpp>

namespace dariadb {
namespace storage {
struct IMemoryAllocator {
  struct AllocatedData {
    ChunkHeader *header;
    uint8_t *buffer;
    size_t position;
    AllocatedData(ChunkHeader *h, uint8_t *buf, size_t pos) {
      header = h;
      buffer = buf;
      position = pos;
    }
    AllocatedData() {
      header = nullptr;
      buffer = nullptr;
      position = std::numeric_limits<size_t>::max();
    }
  };

  const AllocatedData EMPTY =
      AllocatedData(nullptr, nullptr, std::numeric_limits<size_t>::max());

  std::atomic_size_t _allocated; /// already allocated count of chunks.
  uint32_t _chunkSize;           /// size of chunk

  IMemoryAllocator() { _allocated = size_t(0); }
  virtual AllocatedData allocate() = 0;
  virtual void free(const AllocatedData &d) = 0;
};
using IMemoryAllocator_Ptr = std::shared_ptr<IMemoryAllocator>;
/*
struct MemChunkAllocator : public utils::NonCopy {
  struct AllocatedData {
    ChunkHeader *header;
    uint8_t *buffer;
    size_t position;
    AllocatedData(ChunkHeader *h, uint8_t *buf, size_t pos) {
      header = h;
      buffer = buf;
      position = pos;
    }
    AllocatedData() {
      header = nullptr;
      buffer = nullptr;
      position = std::numeric_limits<size_t>::max();
    }
  };

  const AllocatedData EMPTY =
      AllocatedData(nullptr, nullptr, std::numeric_limits<size_t>::max());

  size_t _one_chunk_size;
  size_t _maxSize;     /// max size in bytes)
  uint32_t _chunkSize; /// size of chunk
  size_t _capacity;    /// max size in chunks
  std::atomic_size_t _allocated;   /// already allocated count of chunks.

  boost::lockfree::queue<size_t> _free_list;

  EXPORT MemChunkAllocator(size_t maxSize, uint32_t bufferSize);
  MemChunkAllocator(const MemChunkAllocator &) = delete;
  EXPORT ~MemChunkAllocator();
  EXPORT AllocatedData allocate();
  EXPORT void free(const AllocatedData &d);
};*/

struct MemChunkAllocator : public utils::NonCopy, public IMemoryAllocator {
  size_t _one_chunk_size;
  size_t _maxSize; /// max size in bytes)

  ChunkHeader *_headers;
  uint8_t *_buffers;
  uint8_t *_region;
  size_t _capacity; /// max size in chunks

  boost::lockfree::queue<size_t> _free_list;

  EXPORT MemChunkAllocator(size_t maxSize, uint32_t bufferSize);
  MemChunkAllocator(const MemChunkAllocator &) = delete;
  EXPORT ~MemChunkAllocator();
  EXPORT AllocatedData allocate() override;
  EXPORT void free(const AllocatedData &d) override;
};
}
}
