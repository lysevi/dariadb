#pragma once

#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/memstorage/allocators.h>
#include <list>
#include <memory>

namespace dariadb {
namespace storage {

struct TimeTrack;
struct MemChunk : public ZippedChunk {
  ChunkHeader *index_ptr;
  uint8_t *buffer_ptr;
  MemChunkAllocator::AllocatedData _a_data;
  TimeTrack *_track; /// init in TimeTrack
  size_t in_disk_count;

  MemChunk(ChunkHeader *index, uint8_t *buffer, size_t size, const Meas &first_m);
  MemChunk(ChunkHeader *index, uint8_t *buffer);
  ~MemChunk();
  //bool already_in_disk() const; // STRATEGY::CACHE, true - if already writed to disk.
};

using MemChunk_Ptr = std::shared_ptr<MemChunk>;
using MemChunkList = std::list<MemChunk_Ptr>;
}
}