#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/utils/crc.h>
#include <algorithm>

#include <cstring>

using namespace dariadb;
using namespace dariadb::utils;
using namespace dariadb::storage;
using namespace dariadb::compression;

Chunk_Ptr Chunk::create(ChunkHeader *hdr, uint8_t *buffer, uint32_t _size, const Meas &first_m) {
	return Chunk_Ptr{ new Chunk(hdr,buffer,_size,first_m) };
}

Chunk_Ptr Chunk::open(ChunkHeader *hdr, uint8_t *buffer) {
	return Chunk_Ptr{ new Chunk(hdr,buffer) };
}

Chunk::Chunk(ChunkHeader *hdr, uint8_t *buffer)
    : c_writer(std::make_shared<ByteBuffer>(Range{buffer, buffer + hdr->size})) {
  header = hdr;
  _buffer_t = buffer;
  is_owner = false;

  bw = c_writer.getBinaryBuffer();
  bw->set_pos(header->bw_pos);
}

Chunk::Chunk(ChunkHeader *hdr, uint8_t *buffer, uint32_t _size, const Meas &first_m)
    : c_writer(std::make_shared<ByteBuffer>(Range{buffer, buffer + _size})) {
  _buffer_t = buffer;
  header = hdr;
  header->size = _size;

  header->count = 0;
  header->set_first(first_m);
  header->set_last(first_m);
  header->minTime = first_m.time;
  header->maxTime = first_m.time;
  header->flag_bloom = dariadb::storage::bloom_empty<dariadb::Flag>();

  std::fill(_buffer_t, _buffer_t + header->size, 0);
  is_owner = false;

  bw = c_writer.getBinaryBuffer();
  bw->reset_pos();
  header->bw_pos = uint32_t(bw->pos());

  c_writer.append(header->first());

  header->flag_bloom = dariadb::storage::bloom_add(header->flag_bloom, first_m.flag);
}

Chunk::~Chunk() {
  this->bw = nullptr;
  if (is_owner) {
    delete[] this->_buffer_t;
    delete this->header;
  }
}

bool Chunk::checkId(const Id &id) {
  if (header->meas_id != id) {
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

void Chunk::close() {
  header->crc = this->calcChecksum();
  ENSURE(header->crc != 0);
}

void Chunk::updateChecksum(ChunkHeader &hdr, u8vector buff) {
  hdr.crc = utils::crc32(buff, hdr.size);
}

uint32_t Chunk::calcChecksum(ChunkHeader &hdr, u8vector buff) {
  return utils::crc32(buff, hdr.size);
}
uint32_t Chunk::calcChecksum() {
  return Chunk::calcChecksum(*header, this->_buffer_t);
}

/// return - count of skipped bytes.
uint32_t Chunk::compact(ChunkHeader *hdr) {
  if (hdr->bw_pos == 0) { // full chunk
    return 0;
  }
  auto cur_chunk_buf_size = hdr->size - hdr->bw_pos + 1;
  auto skip_count = hdr->size - (uint32_t)cur_chunk_buf_size;
  hdr->size = (uint32_t)cur_chunk_buf_size;
  return skip_count;
}

uint32_t Chunk::getChecksum() {
  return header->crc;
}

bool Chunk::isFull() const {
  return c_writer.isFull();
}

bool Chunk::append(const Meas &m) {
  auto t_f = this->c_writer.append(m);

  if (!t_f) {
    this->close();
    ENSURE(c_writer.isFull());
    return false;
  } else {
    header->bw_pos = uint32_t(bw->pos());

    header->count++;
    header->minTime = std::min(header->minTime, m.time);
    header->maxTime = std::max(header->maxTime, m.time);
    header->flag_bloom = dariadb::storage::bloom_add(header->flag_bloom, m.flag);
    header->set_last(m);

    return true;
  }
}

class ChunkReader : public Chunk::IChunkReader {
public:
  virtual Meas readNext() override {
    ENSURE(!is_end());

    if (_is_first) {
      _is_first = false;
      return _chunk->header->first();
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

Chunk::ChunkReader_Ptr Chunk::getReader() {
  auto raw_res = new ChunkReader;
  raw_res->count = this->header->count;
  raw_res->_chunk = this->shared_from_this();
  raw_res->_is_first = true;
  raw_res->bw = std::make_shared<compression::ByteBuffer>(this->bw->get_range());
  raw_res->bw->reset_pos();
  raw_res->_reader =
      std::make_shared<CopmressedReader>(raw_res->bw, this->header->first());

  Chunk::ChunkReader_Ptr result{raw_res};
  return result;
}
