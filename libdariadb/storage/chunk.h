#pragma once

#include <libdariadb/compression/bytebuffer.h>
#include <libdariadb/compression/compression.h>
#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/utils/async/locker.h>
#include <libdariadb/utils/utils.h>

#include <map>
#include <set>
#include <unordered_map>
#include <vector>

namespace dariadb {
namespace storage {
#pragma pack(push, 1)
struct ChunkHeader {
  uint64_t id; // chunk id;
  Meas first, last;
  Time minTime, maxTime;
  uint64_t flag_bloom;
  uint32_t count;
  uint32_t bw_pos;

  uint32_t size;
  uint32_t crc;

  uint64_t offset_in_page;
};
#pragma pack(pop)

class Chunk: public std::enable_shared_from_this<Chunk> {
public:
  class IChunkReader {
  public:
    virtual Meas readNext() = 0;
    virtual bool is_end() const = 0;
    virtual ~IChunkReader() {}
  };

  using ChunkReader_Ptr = std::shared_ptr<Chunk::IChunkReader>;

  typedef uint8_t *u8vector;

  EXPORT Chunk(ChunkHeader *hdr, uint8_t *buffer, uint32_t _size, const Meas &first_m);
  EXPORT Chunk(ChunkHeader *hdr, uint8_t *buffer);
  EXPORT ~Chunk();

  EXPORT bool append(const Meas &m);
  EXPORT bool isFull() const ;
  EXPORT ChunkReader_Ptr getReader();
  EXPORT void close();
  EXPORT uint32_t calcChecksum();
  EXPORT uint32_t getChecksum();
  EXPORT virtual bool checkId(const Id &id);
  EXPORT virtual bool checkChecksum();
  EXPORT bool checkFlag(const Flag &f);

  EXPORT static void updateChecksum(ChunkHeader&hdr, u8vector buff);
  EXPORT static uint32_t calcChecksum(ChunkHeader&hdr, u8vector buff);
  ///return - count of skipped bytes.
  EXPORT static uint32_t compact(ChunkHeader*hdr);
  ChunkHeader *header;
  u8vector _buffer_t;

  compression::ByteBuffer_Ptr bw;
  compression::CopmressedWriter c_writer;
  bool is_owner; //true - dealloc memory for header and buffer.
  
};

typedef std::shared_ptr<Chunk> Chunk_Ptr;
typedef std::list<Chunk_Ptr> ChunksList;
typedef std::map<Id, Chunk_Ptr> IdToChunkMap;
typedef std::map<Id, ChunksList> ChunkMap;
typedef std::unordered_map<Id, Chunk_Ptr> IdToChunkUMap;

}
}
