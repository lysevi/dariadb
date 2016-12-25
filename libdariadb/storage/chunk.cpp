#include <libdariadb/storage/chunk.h>
#include <libdariadb/utils/crc.h>
#include <libdariadb/storage/bloom_filter.h>
#include <algorithm>
#include <cassert>
#include <cstring>

using namespace dariadb;
using namespace dariadb::utils;
using namespace dariadb::storage;
using namespace dariadb::compression;

std::ostream &dariadb::storage::operator<<(std::ostream &stream, const CHUNK_KIND &b) {
  switch (b) {
  case CHUNK_KIND::Simple:
    stream << "CHUNK_KIND::Simple";
    break;
  case CHUNK_KIND::Compressed:
    stream << "CHUNK_KIND::Compressed";
    break;
  }
  return stream;
}

Chunk::Chunk(ChunkHeader *hdr, uint8_t *buffer) {
  header = hdr;
  _buffer_t = buffer;
  is_owner = false;
}

Chunk::Chunk(ChunkHeader *hdr, uint8_t *buffer, size_t _size, const Meas &first_m) {
  _buffer_t = buffer;
  header = hdr;
  header->size = _size;

  header->count = 0;
  header->first = first_m;
  header->last = first_m;
  header->minTime = first_m.time;
  header->maxTime = first_m.time;
  header->flag_bloom = dariadb::storage::bloom_empty<dariadb::Flag>();

  std::fill(_buffer_t, _buffer_t + header->size, 0);
  is_owner = false;
}

Chunk::~Chunk() {
  this->bw = nullptr;
  if (is_owner) {
	  delete[] this->_buffer_t;
	  delete this->header;
  }
}

bool Chunk::checkId(const Id &id) {
  if (header->first.id!= id) {
    return false;
  }
  return true;
}

bool Chunk::checkFlag(const Flag &f) {
  if (f != 0) {
    if (!dariadb::storage::bloom_check(header->flag_bloom, f)) {
      return false;
    }
  }
  return true;
}

bool Chunk::checkChecksum() {
  auto exists = getChecksum();
  auto calculated = calcChecksum();
  return exists == calculated;
}

ZippedChunk::ZippedChunk(ChunkHeader *index, uint8_t *buffer, size_t _size, const Meas &first_m)
    : Chunk(index, buffer, _size, first_m),
	c_writer(std::make_shared<ByteBuffer>(Range{ _buffer_t, _buffer_t + index->size }))
	{
  header->kind = CHUNK_KIND::Compressed;
  
  bw = c_writer.getBinaryBuffer();
  bw->reset_pos();
  header->bw_pos = uint32_t(bw->pos());

  c_writer.append(header->first);

  header->flag_bloom = dariadb::storage::bloom_add(header->flag_bloom, first_m.flag);
}

ZippedChunk::ZippedChunk(ChunkHeader *index, uint8_t *buffer) 
	: Chunk(index, buffer),
	c_writer(std::make_shared<ByteBuffer>(Range{ _buffer_t, _buffer_t + index->size }))
{
  assert(index->kind == CHUNK_KIND::Compressed);
  bw = c_writer.getBinaryBuffer();
  bw->set_pos(header->bw_pos);
}

ZippedChunk::~ZippedChunk() {}

void ZippedChunk::close() {
  header->crc = this->calcChecksum();
  assert(header->crc != 0);
}

uint32_t ZippedChunk::calcChecksum() {
  return utils::crc32(this->_buffer_t, this->header->size);
}

uint32_t ZippedChunk::getChecksum() {
  return header->crc;
}

bool ZippedChunk::isFull() const {
  return c_writer.isFull();
}

bool ZippedChunk::append(const Meas &m) {
  auto t_f = this->c_writer.append(m);

  if (!t_f) {
    this->close();
    assert(c_writer.isFull());
    return false;
  } else {
    header->bw_pos = uint32_t(bw->pos());

    header->count++;
    header->minTime = std::min(header->minTime, m.time);
    header->maxTime = std::max(header->maxTime, m.time);
    header->flag_bloom = dariadb::storage::bloom_add(header->flag_bloom, m.flag);
    header->last = m;

    return true;
  }
}

class ZippedChunkReader : public Chunk::IChunkReader {
public:
  virtual Meas readNext() override {
    assert(!is_end());

    if (_is_first) {
      _is_first = false;
      return _chunk->header->first;
    }
    --count;
    return _reader->read();
  }

  bool is_end() const override { return count == 0 && !_is_first; }

  size_t count;
  bool _is_first = true;
  Chunk_Ptr _chunk;
  std::shared_ptr<ByteBuffer> bw;
  std::shared_ptr<CopmressedReader> _reader;
};

Chunk::ChunkReader_Ptr ZippedChunk::getReader() {
  auto raw_res = new ZippedChunkReader;
  raw_res->count = this->header->count;
  raw_res->_chunk = this->shared_from_this();
  raw_res->_is_first = true;
  raw_res->bw = std::make_shared<compression::ByteBuffer>(this->bw->get_range());
  raw_res->bw->reset_pos();
  raw_res->_reader = std::make_shared<CopmressedReader>(raw_res->bw, this->header->first);

  Chunk::ChunkReader_Ptr result{raw_res};
  return result;
}
