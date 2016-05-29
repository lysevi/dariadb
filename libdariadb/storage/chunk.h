#pragma once

#include "../compression.h"
#include "../compression/binarybuffer.h"
#include "../meas.h"
#include "../utils/locker.h"
#include "../utils/utils.h"
#include "../utils/lru.h"

#include <map>
#include <set>
#include <unordered_map>
namespace dariadb {
namespace storage {

#pragma pack(push, 1)
struct ChunkIndexInfo {
  uint64_t id;
  bool is_init;
  bool is_zipped;
  Meas first, last;
  Time minTime, maxTime;
  dariadb::Id id_bloom;
  dariadb::Flag flag_bloom;
  uint32_t count;
  uint32_t bw_pos;
  uint8_t bw_bit_num;
  bool is_readonly;
  compression::CopmressedWriter::Position writer_position; // TODO move from this.

  size_t size;
};
#pragma pack(pop)

class Chunk {
public:
  class Reader {
  public:
    virtual Meas readNext() = 0;
    virtual bool is_end() const = 0;
    virtual ~Reader() {}
  };

  using Reader_Ptr = std::shared_ptr<Chunk::Reader>;

  typedef uint8_t *u8vector;

  Chunk(ChunkIndexInfo *index, uint8_t *buffer, size_t _size, Meas first_m);
  Chunk(ChunkIndexInfo *index, uint8_t *buffer);
  virtual ~Chunk();

  virtual bool append(const Meas &m) = 0;
  virtual bool is_full() const = 0;
  virtual Reader_Ptr get_reader() = 0;
  bool check_flag(const Flag &f);
  bool check_id(const Id &id);
  // TODO remove?
  void lock() { _locker.lock(); }
  void unlock() { _locker.unlock(); }

  ChunkIndexInfo *info;
  u8vector _buffer_t;

  utils::Locker _locker;
  compression::BinaryBuffer_Ptr bw;

  bool should_free; // chunk dtor must delete[] resource.
};

class ZippedChunk : public Chunk, public std::enable_shared_from_this<Chunk> {
public:
  ZippedChunk(ChunkIndexInfo *index, uint8_t *buffer, size_t _size, Meas first_m);
  ZippedChunk(ChunkIndexInfo *index, uint8_t *buffer);
  ~ZippedChunk();
  bool is_full() const override { return c_writer.is_full(); }
  bool append(const Meas &m) override;
  Reader_Ptr get_reader() override;
  utils::Range range;
  compression::CopmressedWriter c_writer;
};

typedef std::shared_ptr<Chunk> Chunk_Ptr;
typedef std::list<Chunk_Ptr> ChunksList;
typedef std::map<Id, Chunk_Ptr> IdToChunkMap;
typedef std::map<Id, ChunksList> ChunkMap;
typedef std::unordered_map<Id, Chunk_Ptr> IdToChunkUMap;

class ChunkCache{
    ChunkCache(size_t size);
public:
static void start(size_t size);
static void stop();
static ChunkCache* instance();
void append(const Chunk_Ptr&chptr);
bool find(const uint64_t id, Chunk_Ptr&chptr)const;
protected:
static std::unique_ptr<ChunkCache> _instance;
mutable std::mutex _locker;
size_t _size;
mutable utils::LRU<uint64_t, Chunk_Ptr> _chunks;
};

}
}
