#include <libdariadb/storage/allocators.h>
#include <memory>
#include <cstring>

using namespace dariadb;
using namespace dariadb::storage;

MemChunkAllocator::MemChunkAllocator(size_t maxSize, size_t bufferSize) {
  _maxSize = maxSize;
  _chunkSize = bufferSize;
  _allocated = size_t(0);
  size_t one_chunk_size = sizeof(ChunkHeader) + bufferSize;
  _capacity = (int)(float(_maxSize) / one_chunk_size);
  size_t buffers_size=_capacity * (_chunkSize+sizeof(ChunkHeader));

  _region=new uint8_t[buffers_size];
  _headers=reinterpret_cast<ChunkHeader*>(_region);
  _buffers = _region+_capacity * sizeof(ChunkHeader);

  memset(_region,0,buffers_size);
  for(size_t i=0;i<_capacity;++i){
      _free_list.push_back(i);
  }
}

MemChunkAllocator::~MemChunkAllocator() {
  delete[] _region;
}

MemChunkAllocator::AllocatedData MemChunkAllocator::allocate() {
  _locker.lock();
  if(_free_list.empty()){
      _locker.unlock();
      return EMPTY;
  }
  auto pos=_free_list.front();
  _free_list.pop_front();
  _allocated++;
  _locker.unlock();
  return AllocatedData(&_headers[pos], &_buffers[pos * _chunkSize], pos);
}

void MemChunkAllocator::free(const MemChunkAllocator::AllocatedData &d) {
	auto header = d.header;
	auto buffer = d.buffer;
	auto position = d.position;
  memset(header, 0, sizeof(ChunkHeader));
  memset(buffer, 0, _chunkSize);
  _locker.lock();
  _allocated--;
  _free_list.push_back(position);
  _locker.unlock();
}
