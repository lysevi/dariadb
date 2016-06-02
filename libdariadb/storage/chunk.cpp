#include "chunk.h"
#include "bloom_filter.h"
#include <algorithm>
#include <cassert>
#include <cstring>

using namespace dariadb;
using namespace dariadb::utils;
using namespace dariadb::storage;
using namespace dariadb::compression;


std::unique_ptr<ChunkCache> ChunkCache::_instance = nullptr;


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
  info->is_sorted = true;
  info->count = 0;
  info->first = first_m;
  info->last = first_m;
  info->minTime = first_m.time;
  info->maxTime = first_m.time;
  info->minId = first_m.id;
  info->maxId = first_m.id;
  info->flag_bloom = dariadb::storage::bloom_empty<dariadb::Flag>();
  info->id_bloom = dariadb::storage::bloom_empty<dariadb::Id>();

  std::fill(_buffer_t, _buffer_t + info->size, 0);
}

Chunk::~Chunk() {
  if (should_free) {
    delete info;
    delete[] _buffer_t;
  }
  this->bw = nullptr;
}

bool Chunk::check_id(const Id &id) {
  if (!dariadb::storage::bloom_check(info->id_bloom, id)) {
    return false;
  }
  return inInterval(info->minId, info->maxId, id);
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

  info->id_bloom = dariadb::storage::bloom_add(info->id_bloom, first_m.id);
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

void ZippedChunk::close(){
    info->is_readonly = true;
}

bool ZippedChunk::append(const Meas &m) {
  if (!info->is_init || info->is_readonly) {
    throw MAKE_EXCEPTION("(!is_not_free || is_readonly)");
  }

  std::lock_guard<utils::Locker> lg(_locker);
  auto t_f = this->c_writer.append(m);
  info->writer_position = c_writer.get_position();

  if (!t_f) {
    this->close();
    assert(c_writer.is_full());
    return false;
  } else {
    info->bw_pos = uint32_t(bw->pos());
    info->bw_bit_num = bw->bitnum();

    info->count++;
	if (m.time < info->last.time) {
		info->is_sorted = false;
	}
    info->minTime = std::min(info->minTime, m.time);
    info->maxTime = std::max(info->maxTime, m.time);
	info->minId = std::min(info->minId, m.id);
	info->maxId = std::max(info->maxId, m.id);
	info->flag_bloom = dariadb::storage::bloom_add(info->flag_bloom, m.flag);
    info->id_bloom = dariadb::storage::bloom_add(info->id_bloom, m.id);
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


class SortedReader : public Chunk::Reader {
public:
	virtual Meas readNext() override {
		assert(!is_end());
		auto res = *_iter;
		++_iter;
		return res;
	}

	bool is_end() const override { return _iter==values.cend(); }

	dariadb::Meas::MeasArray values;
	dariadb::Meas::MeasArray::const_iterator _iter;
};

Chunk::Reader_Ptr ZippedChunk::get_reader() {
	if (info->is_sorted) {
		auto raw_res = new ZippedChunkReader;
		raw_res->count = this->info->count;
		raw_res->_chunk = this->shared_from_this();
		raw_res->_is_first = true;
		raw_res->bw = std::make_shared<BinaryBuffer>(this->bw->get_range());
		raw_res->bw->reset_pos();
		raw_res->_reader = std::make_shared<CopmressedReader>(raw_res->bw, this->info->first);

		Chunk::Reader_Ptr result{ raw_res };
		return result;
	}
	else {
		Meas::MeasList res_set;
		auto cp_bw = std::make_shared<BinaryBuffer>(this->bw->get_range());
		cp_bw->reset_pos();
		auto c_reader = std::make_shared<CopmressedReader>(cp_bw, this->info->first);
		res_set.push_back(info->first);
		for (size_t i = 0; i < info->count; ++i) {
			res_set.push_back(c_reader->read());
		}
		assert(res_set.size() == info->count+1); //compressed+first
		auto raw_res = new SortedReader;
		raw_res->values=dariadb::Meas::MeasArray{ res_set.begin(),res_set.end() };
		std::sort(raw_res->values.begin(), raw_res->values.end(), dariadb::meas_time_compare_less());
		raw_res->_iter = raw_res->values.begin();
		Chunk::Reader_Ptr result{ raw_res };
		return result;
	}
}

CrossedChunk::CrossedChunk(ChunksList&clist) :Chunk(nullptr,nullptr), _chunks(clist) {
}

CrossedChunk::~CrossedChunk() {
}

Chunk::Reader_Ptr CrossedChunk::get_reader() {
	Meas::MeasList res_set;
	for (auto c : _chunks) {
		auto cp_bw = std::make_shared<BinaryBuffer>(c->bw->get_range());
		cp_bw->reset_pos();
		auto c_reader = std::make_shared<CopmressedReader>(cp_bw, c->info->first);
		res_set.push_back(c->info->first);
		for (size_t i = 0; i < c->info->count; ++i) {
			res_set.push_back(c_reader->read());
		}
	}
	auto raw_res = new SortedReader;
	raw_res->values = dariadb::Meas::MeasArray{ res_set.begin(),res_set.end() };
	std::sort(raw_res->values.begin(), raw_res->values.end(), dariadb::meas_time_compare_less());
	raw_res->_iter = raw_res->values.begin();
	Chunk::Reader_Ptr result{ raw_res };
	return result;
}

bool CrossedChunk::check_id(const Id &id) {
	for (auto c : _chunks) {
		if (c->check_id(id)) {
			return true;
		}
	}
	return false;
}

ChunkCache::ChunkCache(size_t size) : _chunks(size) {
  _size = size;
}

void ChunkCache::start(size_t size) {
  ChunkCache::_instance = std::unique_ptr<ChunkCache>{new ChunkCache(size)};
}

void ChunkCache::stop() {}

ChunkCache *ChunkCache::instance() {
  return _instance.get();
}

void ChunkCache::append(const Chunk_Ptr &chptr) {
  std::lock_guard<std::mutex> lg(_locker);
  Chunk_Ptr dropped;
  _chunks.put(chptr->info->id, chptr, &dropped);
}

bool ChunkCache::find(const uint64_t id, Chunk_Ptr &chptr) const {
  std::lock_guard<std::mutex> lg(_locker);
  return this->_chunks.find(id, &chptr);
}

namespace dariadb {
namespace storage {
using namespace dariadb::utils;

ChunksList chunk_intercross(const ChunksList &chunks) {
  ChunksList result;
  std::set<uint64_t> in_cross;
  std::vector<Chunk_Ptr> ch_vector{chunks.begin(),chunks.end()};
  std::sort(ch_vector.begin(),ch_vector.end(),
            [](Chunk_Ptr&l,Chunk_Ptr&r){return l->info->minTime<r->info->minTime;});
  for (auto it = ch_vector.begin(); it != ch_vector.end(); ++it) {
    auto c = *it;
    if (in_cross.find(c->info->id) != in_cross.end()) {
      continue;
    }
    dariadb::storage::ChunksList cross;
    bool cur_intercross = false;
    for (auto other_it = it; other_it != ch_vector.end(); ++other_it) {
      auto other = *other_it;
      if (other->info->id == c->info->id) {
        continue;
      }
      if (in_cross.find(other->info->id) != in_cross.end()) {
        continue;
      }

	  auto id_cross = (inInterval(c->info->minId, c->info->maxId, other->info->minId) ||
		  inInterval(c->info->minId, c->info->maxId, other->info->maxId));
      auto in_time_interval=inInterval(c->info->minTime, c->info->maxTime, other->info->minTime) ;
      auto bloom_eq=c->info->id_bloom == other->info->id_bloom;
      auto time_cross=inInterval(c->info->minTime, c->info->maxTime, other->info->minTime);
	  if (id_cross &&
          in_time_interval
          && time_cross
          && bloom_eq) {
        cur_intercross = true;
        cross.push_back(other);
        in_cross.insert(other->info->id);
      }
    }
    if (cur_intercross) {
      cross.push_front(c);
      dariadb::storage::Chunk_Ptr ch{new dariadb::storage::CrossedChunk(cross)};
      result.push_back(ch);
    } else {
      result.push_back(c);
    }
  }
  return result;
}
}
}
