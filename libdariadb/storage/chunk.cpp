#include <algorithm>
#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/cursors.h>
#include <libdariadb/utils/crc.h>

#include <cstring>

using namespace dariadb;
using namespace dariadb::utils;
using namespace dariadb::storage;
using namespace dariadb::compression;

Chunk_Ptr Chunk::create(ChunkHeader *hdr, uint8_t *buffer, uint32_t _size,
                        const Meas &first_m) {
  return Chunk_Ptr{new Chunk(hdr, buffer, _size, first_m)};
}

Chunk_Ptr Chunk::open(ChunkHeader *hdr, uint8_t *buffer) {
  return Chunk_Ptr{new Chunk(hdr, buffer)};
}

Chunk::Chunk(ChunkHeader *hdr, uint8_t *buffer)
    : c_writer(
          std::make_shared<ByteBuffer>(Range{buffer, buffer + hdr->size})) {
  header = hdr;
  ENSURE(header->stat.maxTime != MIN_TIME);
  ENSURE(header->stat.minTime != MAX_TIME);
  _buffer_t = buffer;
  is_owner = false;

  bw = c_writer.getBinaryBuffer();
  bw->set_pos(header->bw_pos);
}

Chunk::Chunk(ChunkHeader *hdr, uint8_t *buffer, uint32_t _size,
             const Meas &first_m)
    : c_writer(std::make_shared<ByteBuffer>(Range{buffer, buffer + _size})) {
  _buffer_t = buffer;
  header = hdr;
  header->size = _size;

  header->set_first(first_m);
  header->set_last(first_m);
  header->stat = Statistic();
  ENSURE(header->stat.maxTime == MIN_TIME);
  ENSURE(header->stat.minTime == MAX_TIME);
  header->stat.update(first_m);

  header->is_sorted = uint8_t(1);

  std::fill(_buffer_t, _buffer_t + header->size, 0);
  is_owner = false;

  bw = c_writer.getBinaryBuffer();
  bw->reset_pos();
  header->bw_pos = uint32_t(bw->pos());

  c_writer.append(header->first());

  ENSURE(header->is_sorted == uint8_t(1));
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
    if (!dariadb::storage::bloom_check(header->stat.flag_bloom, f)) {
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
  hdr->bw_pos = 0;
  return skip_count;
}

uint32_t Chunk::getChecksum() { return header->crc; }

bool Chunk::isFull() const { return c_writer.isFull(); }

bool Chunk::append(const Meas &m) {
  auto t_f = this->c_writer.append(m);

  if (!t_f) {
    this->close();
    ENSURE(c_writer.isFull());
    return false;
  } else {
    if (m.time < header->data_last.time) {
      header->is_sorted = uint8_t(0);
    }
    header->bw_pos = uint32_t(bw->pos());

    header->stat.update(m);
    header->set_last(m);

    return true;
  }
}

class ChunkReader : public ICursor {
public:
  ChunkReader() = delete;

  ChunkReader(size_t count, const Chunk_Ptr &c,
              std::shared_ptr<ByteBuffer> bptr,
              std::shared_ptr<CopmressedReader> compressed_rdr) {
    _top_value_exists = false;
    _is_first = true;
    _count = count;
    _chunk = c;
    _bw = bptr;
    _compressed_rdr = compressed_rdr;

    ENSURE(_chunk->header->stat.minTime <= _chunk->header->stat.maxTime);
  }

  Meas readNext() override {
    ENSURE(!is_end());
    if (_is_first) {
      _is_first = false;
      _top_value_exists = false;
      return _chunk->header->first();
    }
    if (_top_value_exists) {
      _top_value_exists = false;
      return _top_value;
    }
    --_count;
    return _compressed_rdr->read();
  }

  bool is_end() const override {
    return _count == 0 && !_is_first && _top_value_exists == false;
  }

  Meas top() override {
    if (_top_value_exists == false) {
      if (_count != 0 && !_is_first) {
        _top_value = readNext();
        _top_value_exists = true;
      } else {
        if (_is_first) {
          _top_value = _chunk->header->first();
          _top_value_exists = true;
        }
      }
    }
    return _top_value;
  }

  Time minTime() override { return _chunk->header->stat.minTime; }

  Time maxTime() override { return _chunk->header->stat.maxTime; }

  bool _top_value_exists;
  Meas _top_value;
  size_t _count;
  bool _is_first = true;
  Chunk_Ptr _chunk;
  std::shared_ptr<ByteBuffer> _bw;
  std::shared_ptr<CopmressedReader> _compressed_rdr;
};

Cursor_Ptr Chunk::getReader() {
  auto b_ptr = std::make_shared<compression::ByteBuffer>(this->bw->get_range());
  auto raw_res = new ChunkReader(
      this->header->stat.count - 1, shared_from_this(), b_ptr,
      std::make_shared<CopmressedReader>(b_ptr, this->header->first()));

  Cursor_Ptr result{raw_res};

  if (!header->is_sorted) {
    MeasArray ma(header->stat.count);
    size_t pos = 0;

    while (!result->is_end()) {
      auto v = result->readNext();
      ma[pos++] = v;
    }

    std::sort(ma.begin(), ma.end(), meas_time_compare_less());
    ENSURE(ma.front().time <= ma.back().time);

    FullCursor *fr = new FullCursor(ma);
    return Cursor_Ptr{fr};
  } else {
    return result;
  }
}

Statistic Chunk::stat(Time from, Time to) {
  if (inInterval(from, to, header->stat.minTime) &&
      inInterval(from, to, header->stat.maxTime)) {
    return header->stat;
  } else {
    Statistic result;
    auto rdr = getReader();
    while (!rdr->is_end()) {
      auto m = rdr->readNext();
      if (inInterval(from, to, m.time)) {
        result.update(m);
      }
    }
    return result;
  }
}