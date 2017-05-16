#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/cola.h>
#include <libdariadb/utils/cz.h>
#include <libdariadb/utils/in_interval.h>

// TODO remove
#include <iostream>

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
  struct LevelHeader {
    uint8_t num;
    Time minTime;
    Time maxTime;
    size_t size;
    size_t pos;

    bool isFull() const { return pos == size; }
    bool isEmpty() const { return pos == size_t(0); }

    bool inInterval(Time from, Time to) const {
      return utils::inInterval(from, to, maxTime) ||
             utils::inInterval(from, to, minTime) ||
             utils::inInterval(minTime, maxTime, from) ||
             utils::inInterval(minTime, maxTime, to);
    }
  };

  struct IndexHeader {
    Cola::Param params;
    Id measId;
    size_t merge_count; // how many time levels was merged.
    bool is_full : 1;
  };
#pragma pack(pop)
  struct Level {
    LevelHeader *_lvl_header;
    Link *_links;

    bool isFull() const { return _lvl_header->isFull(); }
    bool isEmpty() const { return _lvl_header->isEmpty(); }

    size_t size() const { return _lvl_header->size; }
    void clear() {
      std::fill_n(_links, _lvl_header->size, Link());
      _lvl_header->maxTime = MIN_TIME;
      _lvl_header->minTime = MAX_TIME;
      _lvl_header->pos = size_t();
    }

    bool addLink(const Link &lnk) {
      if (isFull()) {
        std::cout << "append to full level "
                  << "sz:" << _lvl_header->size << " pos:" << _lvl_header->pos
                  << std::endl;
        return false;
      }
      // TODO check to duplicates
      _links[_lvl_header->pos] = lnk;
      _lvl_header->pos++;
      _lvl_header->maxTime = std::max(lnk.max_time, _lvl_header->maxTime);
      _lvl_header->minTime = std::min(lnk.max_time, _lvl_header->minTime);
      return true;
    }

    void queryLink(Time from, Time to, std::list<Link> *result) const {
      ENSURE(result != nullptr);
      std::cout << "queryLink: " << (uint16_t)_lvl_header->num << std::endl;
      // TODO use upper/lower bounds
      for (size_t i = 0; i < _lvl_header->pos; ++i) {
        auto lnk = _links[i];
        if (utils::inInterval(from, to, lnk.max_time)) {
          std::cout << "id: " << lnk.chunk_id << std::endl;
          result->push_back(lnk);
        }
      }
    }
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

    uint8_t *levels_ptr = buffer + sizeof(IndexHeader);

    { // memory level
      LevelHeader *lhdr = reinterpret_cast<LevelHeader *>(levels_ptr);
      lhdr->num = 0;
      lhdr->maxTime = MIN_TIME;
      lhdr->minTime = MAX_TIME;
      lhdr->size = block_in_level(0) * _header->params.B;

      ENSURE(lhdr->pos == size_t());

      levels_ptr += sizeof(LevelHeader);
      Link *links = reinterpret_cast<Link *>(levels_ptr);
      Level l{lhdr, links};
      _memory_level = l;
      levels_ptr += sizeof(Link) * lhdr->size;
    }

    for (uint8_t i = 0; i < p.levels; ++i) {
      LevelHeader *lhdr = reinterpret_cast<LevelHeader *>(levels_ptr);
      lhdr->num = i;
      lhdr->maxTime = MIN_TIME;
      lhdr->minTime = MAX_TIME;
      lhdr->size = block_in_level(i) * _header->params.B;
      ENSURE(lhdr->pos == size_t());

      levels_ptr += sizeof(LevelHeader);
      Link *links = reinterpret_cast<Link *>(levels_ptr);
      Level l{lhdr, links};
      _levels.push_back(l);
      levels_ptr += sizeof(Link) * lhdr->size;
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
      levels_ptr += sizeof(Link) * lhdr->size;
    }

    for (uint8_t i = 0; i < _header->params.levels; ++i) {
      LevelHeader *lhdr = reinterpret_cast<LevelHeader *>(levels_ptr);
      ENSURE(lhdr->num == i);

      levels_ptr += sizeof(LevelHeader);
      Link *links = reinterpret_cast<Link *>(levels_ptr);
      Level l{lhdr, links};
      _levels.push_back(l);
      levels_ptr += sizeof(Link) * lhdr->size;
    }
  }

  uint8_t levels() const { return static_cast<uint8_t>(_levels.size()); }
  Id targetId() const { return _header->measId; }

  static constexpr size_t one_block_size(const size_t B) { return sizeof(Link) * B; }

  static constexpr size_t block_in_level(const size_t lev_num) {
    return (size_t(1) << lev_num);
  }

  static constexpr size_t bytes_in_level(size_t B, size_t lvl) {
    return one_block_size(B) * block_in_level(lvl);
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

  bool addLink(uint64_t address, uint64_t chunk_id, Time maxTime) {
    if (_memory_level.isFull()) {
      if (!merge_levels()) {
        std::cout << "isFUll" << std::endl;
        return false;
      }
    }
    Link lnk{maxTime, chunk_id, address};
    auto result = _memory_level.addLink(lnk);
    ENSURE(result);
    return result;
  }

  size_t calc_outlevel_num() {
    size_t new_merge_count = _header->merge_count + 1;
    return dariadb::utils::ctz(~size_t(0) & new_merge_count);
  }
  /// return false on failure.
  bool merge_levels() {
    size_t outlvl = calc_outlevel_num();
    std::cout << "out level: " << outlvl << std::endl;

    if (outlvl >= _header->params.levels) {
      this->_header->is_full = true;
      return false;
    }

    auto merge_target = _levels[outlvl];

    if (outlvl == size_t(_header->params.levels - 1)) {
      if (merge_target.isFull()) {
        this->_header->is_full = true;
        return false;
      }
    }

    std::vector<Level *> to_merge(outlvl + 1);
    to_merge[0] = &_memory_level;

    for (size_t i = 0; i < outlvl; ++i) {
      to_merge[i + 1] = &_levels[i];
    }

    for (auto l : to_merge) {
      for (size_t link = 0; link < l->size(); ++link) {
        bool add_result = merge_target.addLink(l->_links[link]);
        if (!add_result) {
          THROW_EXCEPTION("engine: level merge error");
        }
      }
      l->clear();
    }

    ++_header->merge_count;

    return true;
  }

  std::list<Link> queryLink(Time from, Time to) const {
    std::list<Link> result;

    if (!_memory_level.isEmpty() && _memory_level._lvl_header->inInterval(from, to)) {
      _memory_level.queryLink(from, to, &result);
    }
    // TODO optimize
    for (const auto &l : _levels) {
      if (!l.isEmpty() && l._lvl_header->inInterval(from, to)) {
        l.queryLink(from, to, &result);
      }
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

bool Cola::addLink(uint64_t address, uint64_t chunk_id, Time maxTime) {
  return _impl->addLink(address, chunk_id, maxTime);
}

std::list<Cola::Link> Cola::queryLink(Time from, Time to) const {
  return _impl->queryLink(from, to);
}