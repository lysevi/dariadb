#pragma once

#include <libdariadb/compression/bytebuffer.h>
#include <libdariadb/compression/compression.h>
#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/utils/locker.h>
#include <libdariadb/utils/utils.h>

#include <map>
#include <set>
#include <unordered_map>
#include <vector>

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

  EXPORT Chunk(ChunkHeader *hdr, uint8_t *buffer, size_t _size, const Meas &first_m);
  EXPORT Chunk(ChunkHeader *hdr, uint8_t *buffer);
  EXPORT virtual ~Chunk();

  virtual bool append(const Meas &m) = 0;
  virtual bool isFull() const = 0;
  virtual ChunkReader_Ptr getReader() = 0;
  virtual void close() = 0;
  virtual uint32_t calcChecksum() = 0;
  virtual uint32_t getChecksum() = 0;
  EXPORT virtual bool checkId(const Id &id);
  EXPORT virtual bool checkChecksum();
  EXPORT bool checkFlag(const Flag &f);

  ChunkHeader *header;
  u8vector _buffer_t;

  compression::ByteBuffer_Ptr bw;

  bool is_owner; //true - dealloc memory for header and buffer.
};

typedef std::shared_ptr<Chunk> Chunk_Ptr;
typedef std::list<Chunk_Ptr> ChunksList;
typedef std::map<Id, Chunk_Ptr> IdToChunkMap;
typedef std::map<Id, ChunksList> ChunkMap;
typedef std::unordered_map<Id, Chunk_Ptr> IdToChunkUMap;

class ZippedChunk : public Chunk, public std::enable_shared_from_this<Chunk> {
public:
  EXPORT ZippedChunk(ChunkHeader *index, uint8_t *buffer, size_t _size,
                     const Meas &first_m);
  EXPORT ZippedChunk(ChunkHeader *index, uint8_t *buffer);
  EXPORT ~ZippedChunk();

  EXPORT bool isFull() const override;
  EXPORT bool append(const Meas &m) override;
  EXPORT void close() override;

  EXPORT uint32_t calcChecksum() override;
  EXPORT uint32_t getChecksum() override;
  EXPORT ChunkReader_Ptr getReader() override;
  compression::CopmressedWriter c_writer;
};
}
}
