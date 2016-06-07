#include "chunk.h"
#include "bloom_filter.h"
#include <algorithm>
#include <cassert>
#include <cstring>

using namespace dariadb;
using namespace dariadb::utils;
using namespace dariadb::storage;
using namespace dariadb::compression;

// std::unique_ptr<ChunkCache> ChunkCache::_instance = nullptr;

Chunk::Chunk(ChunkHeader *hdr, uint8_t *buffer) : _locker{} {
  should_free = false;
  header = hdr;
  _buffer_t = buffer;
}

Chunk::Chunk(ChunkHeader *hdr, uint8_t *buffer, size_t _size, Meas first_m) : _locker() {
  should_free = false;
  hdr->is_init = true;
  _buffer_t = buffer;
  header = hdr;
  header->size = _size;

  header->is_readonly = false;
  header->is_sorted = true;
  header->count = 0;
  header->first = first_m;
  header->last = first_m;
  header->minTime = first_m.time;
  header->maxTime = first_m.time;
  header->minId = first_m.id;
  header->maxId = first_m.id;
  header->flag_bloom = dariadb::storage::bloom_empty<dariadb::Flag>();
  header->id_bloom = dariadb::storage::bloom_empty<dariadb::Id>();

  std::fill(_buffer_t, _buffer_t + header->size, 0);
}

Chunk::~Chunk() {
  if (should_free) {
    delete header;
    delete[] _buffer_t;
  }
  this->bw = nullptr;
}

bool Chunk::check_id(const Id &id) {
  if (!dariadb::storage::bloom_check(header->id_bloom, id)) {
    return false;
  }
  return inInterval(header->minId, header->maxId, id);
}

bool Chunk::check_flag(const Flag &f) {
  if (f != 0) {
    if (!dariadb::storage::bloom_check(header->flag_bloom, f)) {
      return false;
    }
  }
  return true;
}

ZippedChunk::ZippedChunk(ChunkHeader *index, uint8_t *buffer, size_t _size, Meas first_m)
    : Chunk(index, buffer, _size, first_m) {
  header->is_zipped = true;
  using compression::BinaryBuffer;
  range = Range{_buffer_t, _buffer_t + index->size};
  bw = std::make_shared<BinaryBuffer>(range);
  bw->reset_pos();

  header->bw_pos = uint32_t(bw->pos());
  header->bw_bit_num = bw->bitnum();

  c_writer = compression::CopmressedWriter(bw);
  c_writer.append(header->first);
  header->writer_position = c_writer.get_position();

  header->id_bloom = dariadb::storage::bloom_add(header->id_bloom, first_m.id);
}

ZippedChunk::ZippedChunk(ChunkHeader *index, uint8_t *buffer) : Chunk(index, buffer) {
  assert(index->is_zipped);
  range = Range{_buffer_t, _buffer_t + index->size};
  assert(size_t(range.end - range.begin) == index->size);
  bw = std::make_shared<BinaryBuffer>(range);
  bw->set_bitnum(header->bw_bit_num);
  bw->set_pos(header->bw_pos);

  c_writer = compression::CopmressedWriter(bw);
  c_writer.restore_position(index->writer_position);
}

ZippedChunk::~ZippedChunk() {}

void ZippedChunk::close() {
  header->is_readonly = true;
}

bool ZippedChunk::append(const Meas &m) {
  if (!header->is_init || header->is_readonly) {
    throw MAKE_EXCEPTION("(!is_not_free || is_readonly)");
  }

  std::lock_guard<utils::Locker> lg(_locker);
  auto t_f = this->c_writer.append(m);
  header->writer_position = c_writer.get_position();

  if (!t_f) {
    this->close();
    assert(c_writer.is_full());
    return false;
  } else {
    header->bw_pos = uint32_t(bw->pos());
    header->bw_bit_num = bw->bitnum();

    header->count++;
    if (m.time < header->last.time) {
      header->is_sorted = false;
    }
    header->minTime = std::min(header->minTime, m.time);
    header->maxTime = std::max(header->maxTime, m.time);
    header->minId = std::min(header->minId, m.id);
    header->maxId = std::max(header->maxId, m.id);
    header->flag_bloom = dariadb::storage::bloom_add(header->flag_bloom, m.flag);
    header->id_bloom = dariadb::storage::bloom_add(header->id_bloom, m.id);
    header->last = m;

    return true;
  }
}

class ZippedChunkReader : public Chunk::Reader {
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
  std::shared_ptr<BinaryBuffer> bw;
  std::shared_ptr<CopmressedReader> _reader;
};

class SortedReader : public Chunk::Reader {
public:
  virtual Meas readNext() override {
    assert(!is_end());
    auto res = *_iter;
    ++_iter;
    return res;
  }

  bool is_end() const override { return _iter == values.cend(); }

  dariadb::Meas::MeasArray values;
  dariadb::Meas::MeasArray::const_iterator _iter;
};

Chunk::Reader_Ptr ZippedChunk::get_reader() {
  if (header->is_sorted) {
    auto raw_res = new ZippedChunkReader;
    raw_res->count = this->header->count;
    raw_res->_chunk = this->shared_from_this();
    raw_res->_is_first = true;
    raw_res->bw = std::make_shared<BinaryBuffer>(this->bw->get_range());
    raw_res->bw->reset_pos();
    raw_res->_reader =
        std::make_shared<CopmressedReader>(raw_res->bw, this->header->first);

    Chunk::Reader_Ptr result{raw_res};
    return result;
  } else {
    Meas::MeasList res_set;
    auto cp_bw = std::make_shared<BinaryBuffer>(this->bw->get_range());
    cp_bw->reset_pos();
    auto c_reader = std::make_shared<CopmressedReader>(cp_bw, this->header->first);
    res_set.push_back(header->first);
    for (size_t i = 0; i < header->count; ++i) {
      res_set.push_back(c_reader->read());
    }
    assert(res_set.size() == header->count + 1); // compressed+first
    auto raw_res = new SortedReader;
    raw_res->values = dariadb::Meas::MeasArray{res_set.begin(), res_set.end()};
    std::sort(raw_res->values.begin(), raw_res->values.end(),
              dariadb::meas_time_compare_less());
    raw_res->_iter = raw_res->values.begin();
    Chunk::Reader_Ptr result{raw_res};
    return result;
  }
}

// CrossedChunk::CrossedChunk(ChunksList &clist) : Chunk(nullptr, nullptr), _chunks(clist)
// {}

// CrossedChunk::~CrossedChunk() {}

// Chunk::Reader_Ptr CrossedChunk::get_reader() {
//  Meas::MeasList res_set;
//  for (auto c : _chunks) {
//    auto cp_bw = std::make_shared<BinaryBuffer>(c->bw->get_range());
//    cp_bw->reset_pos();
//    auto c_reader = std::make_shared<CopmressedReader>(cp_bw, c->header->first);
//    res_set.push_back(c->header->first);
//    for (size_t i = 0; i < c->header->count; ++i) {
//      res_set.push_back(c_reader->read());
//    }
//  }
//  auto raw_res = new SortedReader;
//  raw_res->values = dariadb::Meas::MeasArray{res_set.begin(), res_set.end()};
//  std::sort(raw_res->values.begin(), raw_res->values.end(),
//            dariadb::meas_time_compare_less());
//  raw_res->_iter = raw_res->values.begin();
//  Chunk::Reader_Ptr result{raw_res};
//  return result;
//}

// bool CrossedChunk::check_id(const Id &id) {
//  for (auto c : _chunks) {
//    if (c->check_id(id)) {
//      return true;
//    }
//  }
//  return false;
//}

// ChunkCache::ChunkCache(size_t size) : _chunks(size) {
//  _size = size;
//}

// void ChunkCache::start(size_t size) {
//  ChunkCache::_instance = std::unique_ptr<ChunkCache>{new ChunkCache(size)};
//}

// void ChunkCache::stop() {}

// ChunkCache *ChunkCache::instance() {
//  return _instance.get();
//}

// void ChunkCache::append(const Chunk_Ptr &chptr) {
//  std::lock_guard<std::mutex> lg(_locker);
//  Chunk_Ptr dropped;
//  _chunks.put(chptr->header->id, chptr, &dropped);
//}

// bool ChunkCache::find(const uint64_t id, Chunk_Ptr &chptr) const {
//  std::lock_guard<std::mutex> lg(_locker);
//  return this->_chunks.find(id, &chptr);
//}

// namespace dariadb {
// namespace storage {
// using namespace dariadb::utils;

// ChunksList chunk_intercross(const ChunksList &chunks) {
//  ChunksList result;
//  std::set<uint64_t> in_cross;
//  std::vector<Chunk_Ptr> ch_vector{chunks.begin(), chunks.end()};
//  std::sort(ch_vector.begin(), ch_vector.end(), [](Chunk_Ptr &l, Chunk_Ptr &r) {
//    return l->header->minTime < r->header->minTime;
//  });
//  for (auto it = ch_vector.begin(); it != ch_vector.end(); ++it) {
//    auto c = *it;
//    if (in_cross.find(c->header->id) != in_cross.end()) {
//      continue;
//    }
//    dariadb::storage::ChunksList cross;
//    bool cur_intercross = false;
//    for (auto other_it = it; other_it != ch_vector.end(); ++other_it) {
//      auto other = *other_it;
//      if (other->header->id == c->header->id) {
//        continue;
//      }
//      if (in_cross.find(other->header->id) != in_cross.end()) {
//        continue;
//      }

//      auto id_cross =
//          (inInterval(c->header->minId, c->header->maxId, other->header->minId) ||
//           inInterval(c->header->minId, c->header->maxId, other->header->maxId));
//      auto in_time_interval =
//          inInterval(c->header->minTime, c->header->maxTime, other->header->minTime);
//      auto bloom_eq = c->header->id_bloom == other->header->id_bloom;
//      auto time_cross =
//          inInterval(c->header->minTime, c->header->maxTime, other->header->minTime);
//      if (id_cross && in_time_interval && time_cross && bloom_eq) {
//        cur_intercross = true;
//        cross.push_back(other);
//        in_cross.insert(other->header->id);
//      }
//    }
//    if (cur_intercross) {
//      cross.push_front(c);
//      dariadb::storage::Chunk_Ptr ch{new dariadb::storage::CrossedChunk(cross)};
//      result.push_back(ch);
//    } else {
//      result.push_back(c);
//    }
//  }
//  return result;
//}
//}
//}
