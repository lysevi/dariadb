#pragma once

#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/memstorage/allocators.h>
#include <list>
#include <memory>

namespace dariadb {
namespace storage {

struct TimeTrack;
struct MemChunk : public Chunk {
  ChunkHeader *index_ptr;
  uint8_t *buffer_ptr;
  MemChunkAllocator::AllocatedData _a_data;
  TimeTrack *_track; /// init in TimeTrack
  bool _is_from_pool;

  MemChunk(bool is_from_pool, ChunkHeader *index, uint8_t *buffer,
           uint32_t size, const Meas &first_m);
  MemChunk(bool is_from_pool, ChunkHeader *index, uint8_t *buffer);
  ~MemChunk();
};

using MemChunk_Ptr = std::shared_ptr<MemChunk>;
using MemChunkList = std::list<MemChunk_Ptr>;
}
}