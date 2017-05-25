#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/cursors.h>
#include <libdariadb/utils/crc.h>
#include <algorithm>

#include <cstring>

using namespace dariadb;
using namespace dariadb::utils;
using namespace dariadb::storage;
using namespace dariadb::compression;

class ChunkReader : public ICursor {
public:
  ChunkReader() = delete;

  ChunkReader(size_t count, const Chunk_Ptr &c, std::shared_ptr<ByteBuffer> bptr,
              std::shared_ptr<CopmressedReader> compressed_rdr) {
    _top_value_exists = true;
    _values_count = count + 1; // packed + first
    _count = count;
    _chunk = c;
    _top_value = _chunk->header->first();
    _bw = bptr;
    _compressed_rdr = compressed_rdr;

    ENSURE(_chunk->header->stat.minTime <= _chunk->header->stat.maxTime);
  }

  Meas readNext() override {
    ENSURE(!is_end());
    ENSURE(_top_value_exists);

    if (_count == size_t(0)) {
      _top_value_exists = false;
      return _top_value;
    }

    auto result = _top_value;
    skipDuplicates(result.time);
    return result;
  }

  void skipDuplicates(const Time t) {
    while (_count > 0) {
      _top_value_exists = true;
      --_count;
      _top_value = _compressed_rdr->read();
      if (_top_value.time != t) {
        break;
      }
    }
  }

  bool is_end() const override { return _count == 0 && !_top_value_exists; }

  Meas top() override {
    if (_top_value_exists == false) {
      THROW_EXCEPTION("logic error: chunkReader::top()");
    }
    return _top_value;
  }

  size_t count() const override { return _values_count; }

  Time minTime() override { return _chunk->header->stat.minTime; }

  Time maxTime() override { return _chunk->header->stat.maxTime; }

  size_t _values_count;
  bool _top_value_exists;
  Meas _top_value;
  size_t _count;
  Chunk_Ptr _chunk;
  std::shared_ptr<ByteBuffer> _bw;
  std::shared_ptr<CopmressedReader> _compressed_rdr;
};

Chunk_Ptr Chunk::create(ChunkHeader *hdr, uint8_t *buffer, uint32_t _size,
                        const Meas &first_m) {
  return std::make_shared<Chunk>(hdr, buffer, _size, first_m);
}

Chunk_Ptr Chunk::open(ChunkHeader *hdr, uint8_t *buffer) {
  return std::make_shared<Chunk>(hdr, buffer);
}

Chunk::Chunk(ChunkHeader *hdr, uint8_t *buffer)
    : c_writer(std::make_shared<ByteBuffer>(Range{buffer, buffer + hdr->size})) {
  header = hdr;
  ENSURE(header->stat.maxTime != MIN_TIME);
  ENSURE(header->stat.minTime != MAX_TIME);
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

void Chunk::close() {}

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
    if (m.time < header->data_last.time) {
      header->is_sorted = uint8_t(0);
    }
    header->bw_pos = uint32_t(bw->pos());

    header->stat.update(m);
    header->set_last(m);

    return true;
  }
}

Cursor_Ptr Chunk::getReader() {
  auto b_ptr = std::make_shared<compression::ByteBuffer>(this->bw->get_range());
  Cursor_Ptr result = std::make_shared<ChunkReader>(
      this->header->stat.count - 1, shared_from_this(), b_ptr,
      std::make_shared<CopmressedReader>(b_ptr, this->header->first()));

  if (!header->is_sorted) {
    MeasArray ma(header->stat.count);
    size_t pos = 0;

    while (!result->is_end()) {
      auto v = result->readNext();
      ma[pos++] = v;
    }
    std::sort(ma.begin(), ma.end(), meas_time_compare_less());
    ENSURE(ma.front().time <= ma.back().time);

    return std::make_shared<FullCursor>(ma);
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