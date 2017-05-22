#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/settings.h>
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

    std::pair<Link, Link> minMax() const {
      return std::make_pair(_links[0], _links[_lvl_header->pos - 1]);
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

  std::pair<VolumeIndex::Link, VolumeIndex::Link> minMax() const {
    auto minResult = Link::makeEmpty();
    auto maxResult = Link::makeEmpty();
    minResult.max_time = MAX_TIME;
    maxResult.max_time = MIN_TIME;
    if (!_memory_level.isEmpty()) {
      auto l_mm = _memory_level.minMax();
      if (l_mm.first.max_time < minResult.max_time) {
        minResult = l_mm.first;
      }

      if (l_mm.second.max_time > maxResult.max_time) {
        maxResult = l_mm.second;
      }
    }

    for (const auto &l : _levels) {
      if (!l.isEmpty()) {
        auto l_mm = l.minMax();
        if (l_mm.first.max_time < minResult.max_time) {
          minResult = l_mm.first;
        }

        if (l_mm.second.max_time > maxResult.max_time) {
          maxResult = l_mm.second;
        }
      }
    }
    return std::make_pair(minResult, maxResult);
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

std::pair<VolumeIndex::Link, VolumeIndex::Link> VolumeIndex::minMax() const {
  return _impl->minMax();
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
    _filename = fname;
  }

  Private(const std::string &fname) {
    _volume = MappedFile::open(fname);
    _data = _volume->data();
    _header = reinterpret_cast<Header *>(_data);
    _filename = fname;
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

  std::string fname() const { return this->_filename; }

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

  std::vector<Chunk_Ptr> queryChunks(Id id, Time from, Time to) const {
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

        auto cptr = chunkByLink(lnk);
        ENSURE(cptr->header->meas_id == id);

        result.emplace_back(cptr);
      }
      std::sort(result.begin(), result.end(), [](const Chunk_Ptr &l, const Chunk_Ptr &r) {
        return l->header->id < r->header->id;
      });
      return result;
    }
  }

  Chunk_Ptr queryChunks(Id id, Time timepoint) const {
    auto fres = _indexes.find(id);
    if (fres == _indexes.end()) {
      return nullptr;
    } else {
      auto lnk = fres->second->queryLink(timepoint);

      auto cptr = chunkByLink(lnk);
      ENSURE(cptr->header->meas_id == id);

      return cptr;
    }
  }

  Chunk_Ptr chunkByLink(const VolumeIndex::Link &lnk) const {
    auto firstByte = lnk.address;
    ChunkHeader *chdr = new ChunkHeader();
    memcpy(chdr, _data + firstByte, sizeof(ChunkHeader));

    uint8_t *byte_buffer = new uint8_t[_header->one_chunk_size];
    memcpy(byte_buffer, _data + firstByte + sizeof(ChunkHeader), _header->one_chunk_size);

    auto cptr = Chunk::open(chdr, byte_buffer);
    cptr->is_owner = true;
#ifdef DOUBLE_CHECKS
    auto rdr = cptr->getReader();
    while (!rdr->is_end()) {
      rdr->readNext();
    }
#endif // DOUBLE_CHECKS
    return cptr;
  }

  std::map<Id, std::pair<Meas, Meas>> loadMinMax() const {
    std::map<Id, std::pair<Meas, Meas>> result;

    for (const auto &i : _indexes) {
      auto mm = i.second->minMax();
      Meas minMeas, maxMeas;
      ENSURE(!mm.first.IsEmpty());

      ChunkHeader chdr;
      memcpy(&chdr, _data + mm.first.address, sizeof(ChunkHeader));

      minMeas = chdr.first();

      memcpy(&chdr, _data + mm.second.address, sizeof(ChunkHeader));
      maxMeas = chdr.last();

      result[i.first] = std::make_pair(minMeas, maxMeas);
    }
    return result;
  }

  bool minMaxTime(Id id, Time *minResult, Time *maxResult) {
    auto fres = _indexes.find(id);
    if (fres == _indexes.end()) {
      return false;
    }
    auto mm = fres->second->minMax();
    *maxResult = std::max(*maxResult, mm.second.max_time);

    ChunkHeader chdr; // TODO store minMax time in index;
    memcpy(&chdr, _data + mm.first.address, sizeof(ChunkHeader));

    *minResult = std::min(*minResult, chdr.stat.minTime);
    return true;
  }

  Time minTime() const {
    Time result = MAX_TIME;
    for (auto i : _indexes) {
      auto lnk = i.second->minMax().first;

      ChunkHeader chdr;
      memcpy(&chdr, _data + lnk.address, sizeof(ChunkHeader));

      result = std::min(chdr.stat.minTime, result);
    }
    return result;
  }

  Time maxTime() const {
    Time result = MIN_TIME;
    for (auto i : _indexes) {
      result = std::max(i.second->minMax().second.max_time, result);
    }
    return result;
  }
  MappedFile::MapperFile_ptr _volume;
  uint8_t *_data;
  Header *_header;
  std::unordered_map<Id, std::shared_ptr<VolumeIndex>> _indexes;
  std::string _filename;
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

std::vector<Chunk_Ptr> Volume::queryChunks(Id id, Time from, Time to) const {
  return _impl->queryChunks(id, from, to);
}

Chunk_Ptr Volume::queryChunks(Id id, Time timepoint) const {
  return _impl->queryChunks(id, timepoint);
}
IdArray Volume::indexes() const {
  return _impl->indexes();
}

std::string Volume::fname() const {
  return _impl->fname();
}

std::map<Id, std::pair<Meas, Meas>> Volume::loadMinMax() const {
  return _impl->loadMinMax();
}

bool Volume::minMaxTime(Id id, Time *minResult, Time *maxResult) const {
  return _impl->minMaxTime(id, minResult, maxResult);
}

Time Volume::minTime() const {
  return _impl->minTime();
}

Time Volume::maxTime() const {
  return _impl->maxTime();
}
/////////// VOLUME MANAGER ///////////

namespace {
const char *VOLUME_EXT = ".vlm";
}

class VolumeManager::Private : public IMeasStorage {
public:
  Private(const EngineEnvironment_ptr env) {
    _env = env;
    _settings = _env->getResourceObject<Settings>(EngineEnvironment::Resource::SETTINGS);
    _chunk_size = _settings->chunk_size.value();
  }

  ~Private() {}

  std::list<std::string> volume_list() {
    // TODO use manifest.
    return utils::fs::ls(_settings->raw_path.value(), ::VOLUME_EXT);
  }

  virtual Status append(const Meas &value) override {
    auto fres = _id2chunk.find(value.id);
    if (fres == _id2chunk.end()) {
      auto target_chunk = create_chunk(value);
      _id2chunk[value.id] = target_chunk;
      return Status(size_t(1));
    } else {
      Chunk_Ptr target_chunk = fres->second;
      if (!target_chunk->append(value)) {
        dropToDisk(target_chunk);
        target_chunk = create_chunk(value);
        _id2chunk[value.id] = target_chunk;
        return Status(size_t(1));
      } else {
        return Status(size_t(1));
      }
    }
  }

  Chunk_Ptr create_chunk(const Meas &value) {
    /// logger_info("engine: vmanager - create chunk for ", value.id);
    ChunkHeader *chdr = new ChunkHeader;
    uint8_t *cbuffer = new uint8_t[_chunk_size];
    auto target_chunk = Chunk::create(chdr, cbuffer, _chunk_size, value);
    target_chunk->is_owner = true;
    return target_chunk;
  }

  void dropToDisk(Chunk_Ptr &c) {
    Chunk::updateChecksum(*c->header, c->_buffer_t);
    // TODO do in disk io thread;
    auto createAndAppendToVolume = [&c, this]() {
      _current_volume = createNewVolume();
      if (!_current_volume->addChunk(c)) {
        THROW_EXCEPTION("logic error!");
      }
    };
    if (_current_volume == nullptr) {
      createAndAppendToVolume();
    } else {
      if (!_current_volume->addChunk(c)) {
        createAndAppendToVolume();
      }
    }
  }

  std::shared_ptr<Volume> createNewVolume() {
    Volume::Params p(_settings->volume_size.value(), _chunk_size,
                     _settings->volume_levels_count.value(), _settings->volume_B.value());
    auto fname = utils::fs::random_file_name(VOLUME_EXT);
    fname = utils::fs::append_path(_settings->raw_path.value(), fname);
    logger_info("engine: vmanager - create new volume ", fname);
    return std::make_shared<Volume>(p, fname);
  }

  // Inherited via IMeasStorage
  virtual Time minTime() override {
    Time result = MAX_TIME;
    for (auto kv : _id2chunk) {
      result = std::min(result, kv.second->header->stat.minTime);
    }
    auto visitor = [=, &result](const Volume *v) {
      logger_info("engine: vmanager - min in ", v->fname());
      result = std::min(result, v->minTime());
    };
    apply_for_each_volume(visitor);
    return result;
  }

  virtual Time maxTime() override {
    Time result = MIN_TIME;
    for (auto kv : _id2chunk) {
      result = std::max(result, kv.second->header->stat.maxTime);
    }
    auto visitor = [=, &result](const Volume *v) {
      logger_info("engine: vmanager - max in ", v->fname());
      result = std::max(result, v->maxTime());
    };
    apply_for_each_volume(visitor);
    return result;
  }

  virtual bool minMaxTime(Id id, Time *minResult, Time *maxResult) override {
    *maxResult = MIN_TIME;
    *minResult = MAX_TIME;
    auto result = false;
    auto fres = this->_id2chunk.find(id);
    if (fres != _id2chunk.end()) {
      result = true;
      *minResult = std::min(*minResult, fres->second->header->stat.minTime);
      *maxResult = std::max(*maxResult, fres->second->header->stat.maxTime);
    }

    auto visitor = [=, &result, &minResult, &maxResult](const Volume *v) {
      logger_info("engine: vmanager - minMaxTime in ", v->fname());
      // TODO implement in volume to decrease reads (don unpack chunk if chunk in
      // [from,to]).
      result = result || v->minMaxTime(id, minResult, maxResult);
    };
    apply_for_each_volume(visitor);
    return result;
  }

  virtual void foreach (const QueryInterval &q, IReadCallback * clbk) override {
    NOT_IMPLEMENTED;
  }

  virtual Id2Cursor intervalReader(const QueryInterval &query) override {
    NOT_IMPLEMENTED;
  }

  virtual Id2Meas readTimePoint(const QueryTimePoint &q) override { NOT_IMPLEMENTED; }

  virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override {
    Id2Meas result;
    for (auto id : ids) {
      auto fres = this->_id2chunk.find(id);
      if (fres != _id2chunk.end()) {
        auto last = fres->second->header->last();
        if (last.inFlag(flag)) {
          result[id] = last;
        }
      } else {
        auto visitor = [=, &result](const Volume *v) {
          logger_info("engine: vmanager - currentValue in ", v->fname());

          auto c = v->queryChunks(id, MAX_TIME);
          if (c != nullptr) {
            auto res_fres = result.find(id);
            auto last = c->header->last();
            if (res_fres == result.end() || last.time > res_fres->second.time) {
              result[id] = last;
            }
          }
        };
        apply_for_each_volume(visitor);
      }
    }
    return result;
  }

  virtual Statistic stat(const Id id, Time from, Time to) override {
    Statistic result;
    auto fres = this->_id2chunk.find(id);
    if (fres != _id2chunk.end()) {
      result.update(fres->second->stat(from, to));
    }
    auto visitor = [=, &result](const Volume *v) {
      logger_info("engine: vmanager - stat in ", v->fname());
      // TODO implement in volume to decrease reads (don unpack chunk if chunk in
      // [from,to]).
      auto chunks = v->queryChunks(id, from, to);
      for (const auto &c : chunks) {
        result.update(c->stat(from, to));
      }
    };
    apply_for_each_volume(visitor);
    return result;
  }

  virtual Id2MinMax_Ptr loadMinMax() override {
    Id2MinMax_Ptr result = std::make_shared<Id2MinMax>();
    auto visitor = [&result](const Volume *v) {
      logger_info("engine: vmanager - loadMinMax in ", v->fname());
      auto mm = v->loadMinMax();
      for (auto kv : mm) {
        auto bucket_it = result->find_bucket(kv.first);
        bucket_it.v->second.updateMax(kv.second.first);
        bucket_it.v->second.updateMax(kv.second.second);
      }
    };

    apply_for_each_volume(visitor);

    for (const auto &kv : _id2chunk) {
      auto fres = result->find_bucket(kv.first);
      fres.v->second.updateMax(kv.second->header->last());
      fres.v->second.updateMax(kv.second->header->first());
    }
    return result;
  }

  void apply_for_each_volume(std::function<void(const Volume *)> visitor) {
    // TODO LRU cache needed.
    auto files = this->volume_list();
    for (auto f : files) {
      if (_current_volume->fname() == f) {
        visitor(_current_volume.get());
      } else {
        Volume v(f);
        visitor(&v);
      }
    }
  }

  std::unordered_map<Id, Chunk_Ptr> _id2chunk; // TODO use striped map;
  EngineEnvironment_ptr _env;
  Settings *_settings;
  uint32_t _chunk_size;
  std::shared_ptr<Volume> _current_volume;
};

VolumeManager::~VolumeManager() {
  _impl = nullptr;
}

VolumeManager_Ptr VolumeManager::create(const EngineEnvironment_ptr env) {
  return VolumeManager_Ptr(new VolumeManager(env));
}

VolumeManager::VolumeManager(const EngineEnvironment_ptr env) : _impl(new Private(env)) {}

Time VolumeManager::minTime() {
  return _impl->minTime();
}

Time VolumeManager::maxTime() {
  return _impl->maxTime();
}

bool VolumeManager::minMaxTime(Id id, Time *minResult, Time *maxResult) {
  return _impl->minMaxTime(id, minResult, maxResult);
}

void VolumeManager::foreach (const QueryInterval &q, IReadCallback * clbk) {
  return _impl->foreach (q, clbk);
}

Id2Cursor VolumeManager::intervalReader(const QueryInterval &query) {
  return _impl->intervalReader(query);
}

Id2Meas VolumeManager::readTimePoint(const QueryTimePoint &q) {
  return _impl->readTimePoint(q);
}

Id2Meas VolumeManager::currentValue(const IdArray &ids, const Flag &flag) {
  return _impl->currentValue(ids, flag);
}

Statistic VolumeManager::stat(const Id id, Time from, Time to) {
  return _impl->stat(id, from, to);
}

Id2MinMax_Ptr VolumeManager::loadMinMax() {
  return _impl->loadMinMax();
}

Status VolumeManager::append(const Meas &value) {
  return _impl->append(value);
}
