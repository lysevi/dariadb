#include <libdariadb/storage/memstorage/allocators.h>
#include <cstring>
#include <memory>

using namespace dariadb;
using namespace dariadb::storage;

MemChunkAllocator::MemChunkAllocator(size_t maxSize, uint32_t bufferSize)
    : _one_chunk_size(sizeof(ChunkHeader) + bufferSize),
      _capacity((int)(float(maxSize) / _one_chunk_size)), _free_list(_capacity) {
  _maxSize = maxSize;
  _chunkSize = bufferSize;
  _allocated = size_t(0);

  size_t buffers_size = _capacity * (_chunkSize + sizeof(ChunkHeader));

  _region = new uint8_t[buffers_size];
  _headers = reinterpret_cast<ChunkHeader *>(_region);
  _buffers = _region + _capacity * sizeof(ChunkHeader);

  memset(_region, 0, buffers_size);
  for (size_t i = 0; i < _capacity; ++i) {
    auto res = _free_list.push(i);
    if (!res) {
      THROW_EXCEPTION("engine: MemChunkAllocator::ctor - bad capacity.");
    }
  }
}

MemChunkAllocator::~MemChunkAllocator() {
  delete[] _region;
}

MemChunkAllocator::AllocatedData MemChunkAllocator::allocate() {
  size_t pos;
  if (!_free_list.pop(pos)) {
    return EMPTY;
  }
  _allocated++;
  return AllocatedData(&_headers[pos], &_buffers[pos * _chunkSize], pos);
}

void MemChunkAllocator::free(const MemChunkAllocator::AllocatedData &d) {
  ENSURE(d.header != nullptr);
  ENSURE(d.buffer != nullptr);
  ENSURE(d.position != EMPTY.position);
  auto header = d.header;
  auto buffer = d.buffer;
  auto position = d.position;
  memset(header, 0, sizeof(ChunkHeader));
  memset(buffer, 0, _chunkSize);

  _allocated--;
  auto res = _free_list.push(position);
  if (!res) {
    THROW_EXCEPTION("engine: MemChunkAllocator::free - bad capacity.");
  }
}
