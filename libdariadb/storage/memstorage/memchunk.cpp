#include <libdariadb/storage/memstorage/memchunk.h>

using namespace dariadb;
using namespace dariadb::storage;

MemChunk::MemChunk(bool is_from_pool, ChunkHeader *index, uint8_t *buffer, uint32_t size,
                   const Meas &first_m, IMemoryAllocator_Ptr _a)
    : Chunk(index, buffer, size, first_m) {
  index_ptr = index;
  buffer_ptr = buffer;
  _track = nullptr;
  _is_from_pool = is_from_pool;
  _allocator = _a;
}

MemChunk::MemChunk(bool is_from_pool, ChunkHeader *index, uint8_t *buffer,
                   IMemoryAllocator_Ptr _a)
    : Chunk(index, buffer) {
  index_ptr = index;
  buffer_ptr = buffer;
  _track = nullptr;
  _is_from_pool = is_from_pool;
  _allocator = _a;
}

MemChunk::~MemChunk() {
  if (!_is_from_pool) {
    delete[] buffer_ptr;
    delete index_ptr;
  } else {
    _allocator->free(_a_data);
  }
}