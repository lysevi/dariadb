#include "chunk.h"
#include "bloom_filter.h"
#include <algorithm>
#include <cassert>
#include <cstring>

using namespace dariadb;
using namespace dariadb::utils;
using namespace dariadb::storage;
using namespace dariadb::compression;

Chunk::Chunk(ChunkIndexInfo *index, uint8_t *buffer) : _locker{} {
  should_free = false;
  info = index;
  _buffer_t = buffer;
}

Chunk::Chunk(ChunkIndexInfo *index, uint8_t *buffer, size_t _size, Meas first_m)
    : _locker() {
  should_free = false;
  index->is_init = true;
  _buffer_t = buffer;
  info = index;
  info->size = _size;

  info->is_readonly = false;
  info->count = 0;
  info->first = first_m;
  info->last = first_m;
  info->minTime = first_m.time;
  info->maxTime = first_m.time;
  info->flag_bloom = dariadb::storage::bloom_empty<dariadb::Flag>();

  std::fill(_buffer_t, _buffer_t + info->size, 0);
}

Chunk::~Chunk() {
  if (should_free) {
    delete info;
    delete[] _buffer_t;
  }
  this->bw = nullptr;
}

bool Chunk::check_flag(const Flag &f) {
  if (f != 0) {
    if (!dariadb::storage::bloom_check(info->flag_bloom, f)) {
      return false;
    }
  }
  return true;
}

ZippedChunk::ZippedChunk(ChunkIndexInfo *index, uint8_t *buffer, size_t _size,
                         Meas first_m)
    : Chunk(index, buffer, _size, first_m) {
  info->is_zipped = true;
  using compression::BinaryBuffer;
  range = Range{_buffer_t, _buffer_t + index->size};
  bw = std::make_shared<BinaryBuffer>(range);
  bw->reset_pos();

  info->bw_pos = uint32_t(bw->pos());
  info->bw_bit_num = bw->bitnum();

  c_writer = compression::CopmressedWriter(bw);
  c_writer.append(info->first);
  info->writer_position = c_writer.get_position();
}

ZippedChunk::ZippedChunk(ChunkIndexInfo *index, uint8_t *buffer) : Chunk(index, buffer) {
  assert(index->is_zipped);
  range = Range{_buffer_t, _buffer_t + index->size};
  assert(size_t(range.end - range.begin) == index->size);
  bw = std::make_shared<BinaryBuffer>(range);
  bw->set_bitnum(info->bw_bit_num);
  bw->set_pos(info->bw_pos);

  c_writer = compression::CopmressedWriter(bw);
  c_writer.restore_position(index->writer_position);
}

ZippedChunk::~ZippedChunk() {}

bool ZippedChunk::append(const Meas &m) {
  if (!info->is_init || info->is_readonly) {
    throw MAKE_EXCEPTION("(!is_not_free || is_readonly)");
  }

  std::lock_guard<utils::Locker> lg(_locker);
  auto t_f = this->c_writer.append(m);
  info->writer_position = c_writer.get_position();

  if (!t_f) {
    info->is_readonly = true;
    assert(c_writer.is_full());
    return false;
  } else {
    info->bw_pos = uint32_t(bw->pos());
    info->bw_bit_num = bw->bitnum();

    info->count++;

    info->minTime = std::min(info->minTime, m.time);
    info->maxTime = std::max(info->maxTime, m.time);
    info->flag_bloom = dariadb::storage::bloom_add(info->flag_bloom, m.flag);
    info->last = m;
    return true;
  }
}

class ZippedChunkReader : public Chunk::Reader {
public:
  virtual Meas readNext() override {
    assert(!is_end());

    if (_is_first) {
      _is_first = false;
      return _chunk->info->first;
    }
    --count;
    return _reader->read();
  }

  bool is_end() const override { return count == 0 && !_is_first; }

  size_t count;
  bool _is_first = true;
  Chunk_Ptr _chunk;
  std::shared_ptr<BinaryBuffer> bw;
  std::shared_ptr<CopmressedReader> _reader;
};

Chunk::Reader_Ptr ZippedChunk::get_reader() {
  auto raw_res = new ZippedChunkReader;
  raw_res->count = this->info->count;
  raw_res->_chunk = this->shared_from_this();
  raw_res->_is_first = true;
  raw_res->bw = std::make_shared<BinaryBuffer>(this->bw->get_range());
  raw_res->bw->reset_pos();
  raw_res->_reader = std::make_shared<CopmressedReader>(raw_res->bw, this->info->first);

  Chunk::Reader_Ptr result{raw_res};
  return result;
}
