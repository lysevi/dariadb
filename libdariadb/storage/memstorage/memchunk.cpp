#include <libdariadb/storage/memstorage/memchunk.h>

using namespace dariadb;
using namespace dariadb::storage;

MemChunk::MemChunk(ChunkHeader *index, uint8_t *buffer, size_t size, const Meas &first_m)
    : ZippedChunk(index, buffer, size, first_m) {
  index_ptr = index;
  buffer_ptr = buffer;
  _track = nullptr;
  in_disk_count = 0;
}

MemChunk::MemChunk(ChunkHeader *index, uint8_t *buffer) : ZippedChunk(index, buffer) {
  index_ptr = index;
  buffer_ptr = buffer;
  _track = nullptr;
}

MemChunk::~MemChunk() {}

//bool MemChunk::already_in_disk()
//    const { // STRATEGY::CACHE, true - if already writed to disk.
//  return in_disk_count == (this->header->count + 1); // compressed + first;
//}