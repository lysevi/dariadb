#pragma once

#include <libdariadb/compression/v2/bytebuffer.h>
#include <libdariadb/compression/v2/compression.h>
#include <libdariadb/meas.h>
#include <libdariadb/utils/locker.h>
#include <libdariadb/utils/lru.h>
#include <libdariadb/utils/utils.h>

#include <map>
#include <set>
#include <unordered_map>
namespace dariadb {
namespace storage {
enum class CHUNK_KIND : uint8_t { Simple, Compressed };

std::ostream &operator<<(std::ostream &stream, const CHUNK_KIND &k);
#pragma pack(push, 1)
struct ChunkHeader {
  uint64_t id; // chunk id;
  bool is_init : 1;
  bool is_readonly : 1;
  CHUNK_KIND kind;
  Meas first, last;
  Time minTime, maxTime;
  uint64_t flag_bloom;
  uint32_t count;
  uint32_t bw_pos;

  size_t size;
  uint32_t crc;

  uint32_t pos_in_page;
  uint64_t offset_in_page;
};
#pragma pack(pop)

class Chunk {
public:
  class IChunkReader {
  public:
    virtual Meas readNext() = 0;
    virtual bool is_end() const = 0;
    virtual ~IChunkReader() {}
  };

  using ChunkReader_Ptr = std::shared_ptr<Chunk::IChunkReader>;

  typedef uint8_t *u8vector;

  Chunk(ChunkHeader *hdr, uint8_t *buffer, size_t _size, Meas first_m);
  Chunk(ChunkHeader *hdr, uint8_t *buffer);
  virtual ~Chunk();

  virtual bool append(const Meas &m) = 0;
  virtual bool is_full() const = 0;
  virtual ChunkReader_Ptr get_reader() = 0;
  virtual bool check_id(const Id &id);
  virtual void close() = 0;
  virtual uint32_t calc_checksum() = 0;
  virtual uint32_t get_checksum() = 0;
  virtual bool check_checksum();
  bool check_flag(const Flag &f);

  ChunkHeader *header;
  u8vector _buffer_t;

  compression::v2::ByteBuffer_Ptr bw;

  bool should_free; // chunk dtor must (delete[]) resource.
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
  ChunkReader_Ptr get_reader() override;
  compression::v2::CopmressedWriter c_writer;
};
}
}
