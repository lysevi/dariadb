#include <libdariadb/storage/memstorage/allocators.h>
#include <cstring>
#include <memory>

using namespace dariadb;
using namespace dariadb::storage;

UnlimitMemoryAllocator::UnlimitMemoryAllocator(uint32_t bufferSize) {
  _allocated = size_t(0);
  _chunkSize = bufferSize;
}

UnlimitMemoryAllocator::~UnlimitMemoryAllocator() {}

UnlimitMemoryAllocator::AllocatedData UnlimitMemoryAllocator::allocate() {
	try {
		auto v = _allocated.fetch_add(1);
		auto buffer = new uint8_t[_chunkSize];
		std::fill_n(buffer, _chunkSize, uint8_t());
		return AllocatedData(new ChunkHeader, buffer, v);
	}
	catch (std::bad_alloc&ex) {
		return EMPTY;
	}
}

void UnlimitMemoryAllocator::free(const UnlimitMemoryAllocator::AllocatedData &d) {
  ENSURE(d.header != nullptr);
  ENSURE(d.buffer != nullptr);
  ENSURE(d.position != EMPTY.position);
  auto header = d.header;
  auto buffer = d.buffer;

#ifdef DOUBLE_CHECKS
  memset(header, 0, sizeof(ChunkHeader));
  memset(buffer, 0, _chunkSize);
#endif

  delete header;
  delete[] buffer;

  _allocated--;
}

RegionChunkAllocator::RegionChunkAllocator(size_t maxSize, uint32_t bufferSize)
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

RegionChunkAllocator::~RegionChunkAllocator() {
  delete[] _region;
}

RegionChunkAllocator::AllocatedData RegionChunkAllocator::allocate() {
  size_t pos;
  if (!_free_list.pop(pos)) {
    return EMPTY;
  }
  _allocated++;
  return AllocatedData(&_headers[pos], &_buffers[pos * _chunkSize], pos);
}

void RegionChunkAllocator::free(const RegionChunkAllocator::AllocatedData &d) {
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
