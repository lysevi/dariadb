#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/cola.h>
using namespace dariadb;
using namespace dariadb::storage;

/**
|VolumeHeader|Index1, Index2, Index3,....-> ... <- ChunkN, Chunk<|
      +-------^
          V
|IndexHeader|Levels|
*/
struct Cola::Private {
#pragma pack(push, 1)
  struct Link {
    Time max_time;
    uint64_t chunk_id;
    uint64_t address;
  };

  struct LevelHeader {
    uint8_t num;
    Time minTime;
    Time maxTime;
    size_t count;
  };

  struct IndexHeader {
    Cola::Param params;
    Id measId;
  };
#pragma pack(pop)
  struct Level {
    LevelHeader *_lvl_header;
    Link *_links;
  };
  /// init index in memory.
  Private(const Cola::Param &p, Id measId, uint8_t *buffer) {
    size_t sz = index_size(p);
    std::fill_n(buffer, sz, uint8_t());
    initPointers(p, measId, buffer);
  }

  Private(uint8_t *buffer) { initPointers(buffer); }

  void initPointers(const Cola::Param &p, Id measId, uint8_t *buffer) {
    _levels.reserve(p.levels);
    _header = reinterpret_cast<IndexHeader *>(buffer);
    _header->params = p;
    _header->measId = measId;
    auto levels_ptr = buffer + sizeof(IndexHeader);

    { // memory level
      LevelHeader *lhdr = reinterpret_cast<LevelHeader *>(levels_ptr);
      lhdr->num = 0;
      lhdr->maxTime = MIN_TIME;
      lhdr->minTime = MAX_TIME;
      lhdr->count = block_in_level(0) * _header->params.B;

      levels_ptr += sizeof(LevelHeader);
      Link *links = reinterpret_cast<Link *>(levels_ptr);
      Level l{lhdr, links};
      _memory_level = l;
      levels_ptr += sizeof(Link) * lhdr->count;
    }

    for (uint8_t i = 0; i < p.levels; ++i) {
      LevelHeader *lhdr = reinterpret_cast<LevelHeader *>(levels_ptr);
      lhdr->num = i;
      lhdr->maxTime = MIN_TIME;
      lhdr->minTime = MAX_TIME;
      lhdr->count = block_in_level(i) * _header->params.B;

      levels_ptr += sizeof(LevelHeader);
      Link *links = reinterpret_cast<Link *>(levels_ptr);
      Level l{lhdr, links};
      _levels.push_back(l);
      levels_ptr += sizeof(Link) * lhdr->count;
    }
  }

  void initPointers(uint8_t *buffer) {
    _header = reinterpret_cast<IndexHeader *>(buffer);
    _levels.reserve(_header->params.levels);

    auto levels_ptr = buffer + sizeof(IndexHeader);

    { // memory level
      LevelHeader *lhdr = reinterpret_cast<LevelHeader *>(levels_ptr);
      levels_ptr += sizeof(LevelHeader);
      Link *links = reinterpret_cast<Link *>(levels_ptr);
      Level l{lhdr, links};
      _memory_level = l;
      ENSURE(_memory_level._lvl_header->num == uint8_t(0));
      levels_ptr += sizeof(Link) * lhdr->count;
    }

    for (uint8_t i = 0; i < _header->params.levels; ++i) {
      LevelHeader *lhdr = reinterpret_cast<LevelHeader *>(levels_ptr);
      ENSURE(lhdr->num == i);

      levels_ptr += sizeof(LevelHeader);
      Link *links = reinterpret_cast<Link *>(levels_ptr);
      Level l{lhdr, links};
      _levels.push_back(l);
      levels_ptr += sizeof(Link) * lhdr->count;
    }
  }

  uint8_t levels() const { return static_cast<uint8_t>(_levels.size()); }
  Id targetId() const { return _header->measId; }

  static size_t one_block_size(size_t B) { return sizeof(Link) * B; }

  static size_t block_in_level(size_t lev_num) { return (size_t(1) << lev_num); }

  static size_t bytes_in_level(size_t B, size_t lvl) {
    auto blocks_count = block_in_level(lvl);
    auto res = one_block_size(B) * blocks_count;
    return res;
  }

  static size_t index_size(const Param &p) {
    size_t result = 0;
    result += sizeof(IndexHeader) + one_block_size(p.B); /// space to _memvalues
    for (size_t lvl = 0; lvl < p.levels; ++lvl) {
      auto cur_level_links = bytes_in_level(p.B, lvl);
      result += sizeof(LevelHeader) + cur_level_links;
    }
    return result;
  }

  IndexHeader *_header;
  Level _memory_level;
  std::vector<Level> _levels;
};

Cola::Cola(const Param &p, Id measId, uint8_t *buffer)
    : _impl(new Cola::Private(p, measId, buffer)) {}

Cola::Cola(uint8_t *buffer) : _impl(new Cola::Private(buffer)) {}

size_t Cola::index_size(const Param &p) {
  return Cola::Private::index_size(p);
}

uint8_t Cola::levels() const {
  return _impl->levels();
}

Id Cola::targetId() const {
  return _impl->targetId();
}