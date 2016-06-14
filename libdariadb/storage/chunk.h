#pragma once

#include "../compression.h"
#include "../compression/binarybuffer.h"
#include "../meas.h"
#include "../utils/locker.h"
#include "../utils/lru.h"
#include "../utils/utils.h"

#include <map>
#include <set>
#include <unordered_map>
namespace dariadb {
namespace storage {

#pragma pack(push, 1)
struct ChunkHeader {
  uint64_t id; // chunk id;
  bool is_init : 1;
  bool is_zipped : 1;
  bool is_sorted : 1;
  bool is_readonly : 1;
  Meas first, last;
  Time minTime, maxTime;
  Id minId, maxId;
  dariadb::Id id_bloom; // TODO remove. one chunk to one id.
  dariadb::Flag flag_bloom;
  uint32_t count;
  uint32_t bw_pos;
  uint8_t bw_bit_num;

  compression::CopmressedWriter::Position writer_position; // TODO move from this.

  size_t size;
  uint32_t crc;
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

  Chunk(ChunkHeader *hdr, uint8_t *buffer, size_t _size, Meas first_m);
  Chunk(ChunkHeader *hdr, uint8_t *buffer);
  virtual ~Chunk();

  virtual bool append(const Meas &m) = 0;
  virtual bool is_full() const = 0;
  virtual Reader_Ptr get_reader() = 0;
  virtual bool check_id(const Id &id);
  virtual void close() = 0;
  virtual uint32_t calc_checksum() = 0;
  virtual uint32_t get_checksum() = 0;
  virtual bool check_checksum();
  bool check_flag(const Flag &f);
  // TODO remove?
  void lock() { _locker.lock(); }
  void unlock() { _locker.unlock(); }

  ChunkHeader *header;
  u8vector _buffer_t;

  utils::Locker _locker;
  compression::BinaryBuffer_Ptr bw;

  bool should_free; // chunk dtor must delete[] resource.
};

typedef std::shared_ptr<Chunk> Chunk_Ptr;
typedef std::list<Chunk_Ptr> ChunksList;
typedef std::map<Id, Chunk_Ptr> IdToChunkMap;
typedef std::map<Id, ChunksList> ChunkMap;
typedef std::unordered_map<Id, Chunk_Ptr> IdToChunkUMap;

class ZippedChunk : public Chunk, public std::enable_shared_from_this<Chunk> {
public:
  ZippedChunk(ChunkHeader *index, uint8_t *buffer, size_t _size, Meas first_m);
  ZippedChunk(ChunkHeader *index, uint8_t *buffer);
  ~ZippedChunk();
  bool is_full() const override { return c_writer.is_full(); }
  bool append(const Meas &m) override;
  void close() override;

  uint32_t calc_checksum() override;
  uint32_t get_checksum() override;
  Reader_Ptr get_reader() override;
  utils::Range range;
  compression::CopmressedWriter c_writer;
};
}
}
