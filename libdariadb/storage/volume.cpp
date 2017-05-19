#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/volume.h>
#include <libdariadb/utils/cz.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/in_interval.h>
#include <libdariadb/utils/utils.h>

#include <cstring>

using namespace dariadb;
using namespace dariadb::storage;

/**
|IndexHeader|Level1|Level2|Level3|...
      +--------+
      |
      V
   LevelHeader|Link1|Link2...
*/
struct VolumeIndex::Private {
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

    bool hasTimePoint(Time tp) const {
      return utils::inInterval(minTime, maxTime, tp) || maxTime <= tp;
    }
  };

  struct IndexHeader {
    VolumeIndex::Param params;
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
        return false;
      }
      // TODO check to duplicates
      _links[_lvl_header->pos] = lnk;
      _lvl_header->pos++;
      _lvl_header->maxTime = std::max(lnk.max_time, _lvl_header->maxTime);
      _lvl_header->minTime = std::min(lnk.max_time, _lvl_header->minTime);
      return true;
    }

    void queryLink(Time from, Time to, std::vector<Link> *result) const {
      ENSURE(result != nullptr);
      // TODO use upper/lower bounds
      for (size_t i = 0; i < _lvl_header->pos; ++i) {
        auto lnk = _links[i];
        if (utils::inInterval(from, to, lnk.max_time) && !lnk.erased) {
          result->push_back(lnk);
        }
      }
    }

    Link queryLink(Time tp) const {
      size_t result = size_t(0);
      // TODO use upper/lower bounds
      for (size_t i = 1; i < _lvl_header->pos; ++i) {
        auto lnk = _links[i];
        if (lnk.max_time <= tp && !lnk.erased) {
          result = i;
        } else {
          if (lnk.max_time > tp) {
            break;
          }
        }
      }
      return _links[result];
    }

    void rm(Time maxTime, uint64_t chunk_id) {
      for (size_t i = 0; i < _lvl_header->pos; ++i) {
        auto lnk = _links[i];
        if (lnk.max_time == maxTime && chunk_id == lnk.chunk_id) {
          _links[i].erased = true;
          break;
        }
      }
    }
  };

  /// init index in memory.
  Private(const VolumeIndex::Param &p, Id measId, uint8_t *buffer) {
    size_t sz = index_size(p);
    std::fill_n(buffer, sz, uint8_t());
    initPointers(p, measId, buffer);
  }

  Private(uint8_t *buffer) { initPointers(buffer); }

  ~Private() {
    _levels.clear();
    _header = nullptr;
  }

  void initPointers(const VolumeIndex::Param &p, Id measId, uint8_t *buffer) {
    _levels.reserve(p.levels);
    _header = reinterpret_cast<IndexHeader *>(buffer);
    _header->params = p;
    _header->measId = measId;

    uint8_t *levels_ptr = buffer + sizeof(IndexHeader);
    initLevelsPointers(p, levels_ptr, [this](LevelHeader *lhdr, uint8_t num) {
      lhdr->num = num;
      lhdr->maxTime = MIN_TIME;
      lhdr->minTime = MAX_TIME;
      lhdr->size = block_in_level(num) * _header->params.B;

      ENSURE(lhdr->pos == size_t());

      auto linkPtr = reinterpret_cast<uint8_t *>(lhdr + 1);
      Link *links = reinterpret_cast<Link *>(linkPtr);
      Level l{lhdr, links};
      return l;
    });
  }

  void initPointers(uint8_t *buffer) {
    _header = reinterpret_cast<IndexHeader *>(buffer);
    _levels.reserve(_header->params.levels);
    uint8_t *levels_ptr = buffer + sizeof(IndexHeader);

    initLevelsPointers(_header->params, levels_ptr, [](LevelHeader *lhdr, uint8_t num) {
      ENSURE(lhdr->num == num);
      auto linkPtr = reinterpret_cast<uint8_t *>(lhdr + 1);
      Link *links = reinterpret_cast<Link *>(linkPtr);
      Level l{lhdr, links};
      return l;
    });
  }

  /**
  p - cola params
  levels_ptr - pointer to a memory after index header
  levelInitFunc - function must return initialized Level.
  */
  void initLevelsPointers(const VolumeIndex::Param &p, uint8_t *levels_ptr,
                          std::function<Level(LevelHeader *, uint8_t)> levelInitFunc) {
    { // memory level
      LevelHeader *lhdr = reinterpret_cast<LevelHeader *>(levels_ptr);
      auto l = levelInitFunc(lhdr, 0);
      _memory_level = l;
      levels_ptr += sizeof(LevelHeader) + sizeof(Link) * lhdr->size;
    }

    for (uint8_t i = 0; i < p.levels; ++i) {
      LevelHeader *lhdr = reinterpret_cast<LevelHeader *>(levels_ptr);
      auto l = levelInitFunc(lhdr, i);
      ENSURE(lhdr->num == i);
      _levels.push_back(l);

      levels_ptr += sizeof(LevelHeader) + sizeof(Link) * lhdr->size;
    }
  }

  uint8_t levels() const { return uint8_t(_levels.size()); }
  Id targetId() const { return _header->measId; }

  static constexpr size_t one_block_size(const size_t B) { return sizeof(Link) * B; }

  static constexpr size_t block_in_level(const size_t lev_num) {
    return (size_t(1) << lev_num);
  }

  static constexpr size_t bytes_in_level(size_t B, size_t lvl) {
    return one_block_size(B) * block_in_level(lvl);
  }

  static uint32_t index_size(const Param &p) {
    uint32_t result = 0;
    result += uint32_t(sizeof(IndexHeader));
    result += uint32_t(sizeof(LevelHeader) + one_block_size(p.B)); /// space to _memvalues
    for (size_t lvl = 0; lvl < p.levels; ++lvl) {
      auto cur_level_links = bytes_in_level(p.B, lvl);
      result += uint32_t(sizeof(LevelHeader) + cur_level_links);
    }
    return result;
  }

  bool addLink(uint64_t address, uint64_t chunk_id, Time maxTime) {
    if (_memory_level.isFull()) {
      if (!merge_levels()) {
        return false;
      }
    }
    Link lnk{maxTime, chunk_id, address, false};
    auto result = _memory_level.addLink(lnk);
    ENSURE(result);
    return result;
  }

  size_t calc_outlevel_num() {
    size_t new_merge_count = _header->merge_count + 1;
    return dariadb::utils::ctz(~size_t(0) & new_merge_count);
  }

  static void sort_links(Link *begin, Link *end) {
    std::sort(begin, end,
              [](const Link &l, const Link &r) { return l.max_time < r.max_time; });
  }

  /// return false on failure.
  bool merge_levels() {
    size_t outlvl = calc_outlevel_num();

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

    sort_links(merge_target._links, merge_target._links + merge_target.size());
    ENSURE(merge_target._links->max_time <=
           (merge_target._links + merge_target.size() - 1)->max_time);
    ++_header->merge_count;

    return true;
  }

  std::vector<Link> queryLink(Time from, Time to) const {
    std::vector<Link> result;

    if (!_memory_level.isEmpty() && _memory_level._lvl_header->inInterval(from, to)) {
      _memory_level.queryLink(from, to, &result);
    }
    // TODO optimize
    for (const auto &l : _levels) {
      if (!l.isEmpty() && l._lvl_header->inInterval(from, to)) {
        l.queryLink(from, to, &result);
      }
    }
    sort_links(result.data(), result.data() + result.size());
    return result;
  }

  Link queryLink(Time tp) const {
    Link result = Link::makeEmpty();
    if (!_memory_level.isEmpty() && _memory_level._lvl_header->hasTimePoint(tp)) {
      result = _memory_level.queryLink(tp);
    }
    // TODO optimize: find level with maximum "hasTimePoint"
    for (const auto &l : _levels) {
      if (!l.isEmpty() && l._lvl_header->hasTimePoint(tp)) {
        auto sub_result = l.queryLink(tp);
        if (sub_result.max_time > result.max_time || result.IsEmpty()) {
          result = sub_result;
        }
      }
    }
    return result;
  }

  void rm(Time maxTime, uint64_t chunk_id) {
    if (!_memory_level.isEmpty() && _memory_level._lvl_header->hasTimePoint(maxTime)) {
      _memory_level.rm(maxTime, chunk_id);
    }
    // TODO optimize
    for (auto &l : _levels) {
      if (!l.isEmpty() && l._lvl_header->hasTimePoint(maxTime)) {
        l.rm(maxTime, chunk_id);
      }
    }
  }
  IndexHeader *_header;
  Level _memory_level;
  std::vector<Level> _levels;
};

VolumeIndex::VolumeIndex(const Param &p, Id measId, uint8_t *buffer)
    : _impl(new VolumeIndex::Private(p, measId, buffer)) {}

VolumeIndex::VolumeIndex(uint8_t *buffer) : _impl(new VolumeIndex::Private(buffer)) {}

VolumeIndex::~VolumeIndex() {
  _impl = nullptr;
}

uint32_t VolumeIndex::index_size(const Param &p) {
  return VolumeIndex::Private::index_size(p);
}

uint8_t VolumeIndex::levels() const {
  return _impl->levels();
}

Id VolumeIndex::targetId() const {
  return _impl->targetId();
}

bool VolumeIndex::addLink(uint64_t address, uint64_t chunk_id, Time maxTime) {
  return _impl->addLink(address, chunk_id, maxTime);
}

std::vector<VolumeIndex::Link> VolumeIndex::queryLink(Time from, Time to) const {
  return _impl->queryLink(from, to);
}

VolumeIndex::Link VolumeIndex::queryLink(Time tp) const {
  return _impl->queryLink(tp);
}

void VolumeIndex::rm(Time maxTime, uint64_t chunk_id) {
  return _impl->rm(maxTime, chunk_id);
}

/////////// VOLUME ///////////
using dariadb::utils::fs::MappedFile;

struct Volume::Private {
#pragma pack(push, 1)
  struct Header {
    uint32_t one_chunk_size; // with header
    VolumeIndex::Param _index_param;
    uint32_t index_pos; // from 0
    uint32_t chunk_pos; // from size
  };
#pragma pack(pop)
  Private(const Params &p, const std::string &fname) {
    ENSURE(p.size >
           (sizeof(Header) + VolumeIndex::index_size(p.indexParams) + p.chunk_size));
    _volume = MappedFile::touch(fname, p.size);
    _data = _volume->data();
    _header = reinterpret_cast<Header *>(_data);
    _header->index_pos = sizeof(Header);
    _header->one_chunk_size = p.chunk_size;
    _header->chunk_pos = p.size;
    _header->_index_param = p.indexParams;
  }

  Private(const std::string &fname) {
    _volume = MappedFile::open(fname);
    _data = _volume->data();
    _header = reinterpret_cast<Header *>(_data);
    auto isz = VolumeIndex::index_size(_header->_index_param);

    auto indexes_count = (_header->index_pos - sizeof(Header)) / isz;
    auto it = _data + sizeof(Header);
    for (size_t i = 0; i < indexes_count; ++i) {
      auto new_index = std::make_shared<VolumeIndex>(it);
      _indexes[new_index->targetId()] = new_index;
      it += isz;
    }
  }

  ~Private() {
    _volume->flush();
    _volume->close();
  }

  IdArray indexes() const {
    IdArray result(_indexes.size());
    size_t result_pos = 0;
    for (const auto &kv : _indexes) {
      result[result_pos++] = kv.first;
    }
    return result;
  }

  bool havePlaceForNewIndex() const {
    auto exist = ((int64_t(_header->chunk_pos) - _header->index_pos -
                   (_header->one_chunk_size + sizeof(ChunkHeader))) >=
                  VolumeIndex::index_size(_header->_index_param));
    return exist;
  }

  bool havePlaceForNewChunk() const {
    auto exist = (int64_t(_header->chunk_pos) - _header->index_pos >=
                  int64_t(_header->one_chunk_size + sizeof(ChunkHeader)));
    return exist;
  }

  uint64_t writeChunk(const Chunk_Ptr &c) {
    _header->chunk_pos -= _header->one_chunk_size + sizeof(ChunkHeader);
    auto firstByte = _header->chunk_pos;
    std::memcpy(_data + firstByte, c->header, sizeof(ChunkHeader));
    std::memcpy(_data + firstByte + sizeof(ChunkHeader), c->_buffer_t, c->header->size);
    return firstByte;
  }

  bool addChunk(const Chunk_Ptr &c) {
    auto fres = _indexes.find(c->header->meas_id);
    if (fres == _indexes.end()) {
      if (havePlaceForNewIndex()) {
        auto address = writeChunk(c);
        auto isz = VolumeIndex::index_size(_header->_index_param);
        auto newIndex = std::make_shared<VolumeIndex>(
            _header->_index_param, c->header->meas_id, _data + _header->index_pos);
        _header->index_pos += isz;
        newIndex->addLink(address, c->header->id, c->header->stat.maxTime);
        _indexes[c->header->meas_id] = newIndex;
        return true;
      }
      return false;
    }
    if (havePlaceForNewChunk()) {
      auto address = writeChunk(c);
      fres->second->addLink(address, c->header->id, c->header->stat.maxTime);
      return true;
    }
    return false;
  }

  std::vector<Chunk_Ptr> queryChunks(Id id, Time from, Time to) {
    std::vector<Chunk_Ptr> result;

    auto fres = _indexes.find(id);
    if (fres == _indexes.end()) {
      return result;
    } else {
      auto links = fres->second->queryLink(from, to);
      if (links.empty()) {
        return result;
      }
      result.reserve(links.size());

      for (auto lnk : links) {
        ENSURE(!lnk.erased);

        auto firstByte = lnk.address;
        ChunkHeader *chdr = new ChunkHeader();
        memcpy(chdr, _data + firstByte, sizeof(ChunkHeader));
        ENSURE(chdr->meas_id == id);

        uint8_t *byte_buffer = new uint8_t[_header->one_chunk_size];
        memcpy(byte_buffer, _data + firstByte + sizeof(ChunkHeader),
               _header->one_chunk_size);

        auto cptr = Chunk::open(chdr, byte_buffer);
        cptr->is_owner = true;
#ifdef DOUBLE_CHECKS
        auto rdr = cptr->getReader();
        while (!rdr->is_end()) {
          auto m = rdr->readNext();
          ENSURE(m.id == id);
        }
#endif // DOUBLE_CHECKS

        result.emplace_back(cptr);
      }
      std::sort(result.begin(), result.end(), [](const Chunk_Ptr &l, const Chunk_Ptr &r) {
        return l->header->id < r->header->id;
      });
      return result;
    }
  }
  MappedFile::MapperFile_ptr _volume;
  uint8_t *_data;
  Header *_header;
  std::unordered_map<Id, std::shared_ptr<VolumeIndex>> _indexes;
};

Volume::Volume(const Params &p, const std::string &fname)
    : _impl(new Private(p, fname)) {}

Volume::Volume(const std::string &fname) : _impl(new Private(fname)) {}

Volume::~Volume() {
  _impl = nullptr;
}

bool Volume::addChunk(const Chunk_Ptr &c) {
  return _impl->addChunk(c);
}

std::vector<Chunk_Ptr> Volume::queryChunks(Id id, Time from, Time to) {
  return _impl->queryChunks(id, from, to);
}

IdArray Volume::indexes() const {
  return _impl->indexes();
}