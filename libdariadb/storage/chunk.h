#pragma once

#include <libdariadb/compression/bytebuffer.h>
#include <libdariadb/compression/compression.h>
#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/utils/async/locker.h>
#include <libdariadb/utils/utils.h>

#include <map>
#include <set>
#include <unordered_map>
#include <vector>

namespace dariadb {
namespace storage {
#pragma pack(push, 1)
struct MeasData {
  Id id;
  Time time;
  Value value;
  Flag flag;
};
struct Statistic {
  Time minTime;
  Time maxTime;

  uint32_t count; /// count of stored values.

  uint64_t flag_bloom;

  Value minValue;

  Value maxValue;

  Value sum;

  Statistic() {
    flag_bloom = bloom_empty<Flag>();
    count = uint32_t(0);
    minTime = MAX_TIME;
    maxTime = MIN_TIME;

    minValue = MAX_VALUE;
    maxValue = MIN_VALUE;

    sum = Value(0);
  }

  void update(const Meas &m) {
    count++;

    minTime = std::min(m.time, minTime);
    maxTime = std::max(m.time, maxTime);

    flag_bloom = bloom_add<Flag>(flag_bloom, m.flag);

    minValue = std::min(m.value, minValue);
    maxValue = std::max(m.value, maxValue);

    sum += m.value;
  }

  void update(const Statistic &st) {
    count += st.count;

    minTime = std::min(st.minTime, minTime);
    maxTime = std::max(st.maxTime, maxTime);

    flag_bloom = bloom_combine(flag_bloom, st.flag_bloom);

    minValue = std::min(st.minValue, minValue);
    maxValue = std::max(st.maxValue, maxValue);

    sum += st.sum;
  }
};

struct ChunkHeader {
  uint64_t id;                    /// chunk id.
  Id meas_id;                     /// measurement id.
  MeasData data_first, data_last; /// data of first and last added measurements.
  uint32_t bw_pos;                /// needed for unpack.

  uint32_t size; /// size of buffer with values.
  uint32_t crc;  /// checksum.

  uint64_t offset_in_page; /// pos in page file.

  Statistic stat;

  Meas first() const {
    Meas m = Meas::empty(meas_id);
    m.flag = data_first.flag;
    m.time = data_first.time;
    m.value = data_first.value;
    return m;
  }

  Meas last() const {
    Meas m = Meas::empty(meas_id);
    m.flag = data_last.flag;
    m.time = data_last.time;
    m.value = data_last.value;
    return m;
  }

  void set_first(Meas m) {
    data_first.flag = m.flag;
    data_first.time = m.time;
    data_first.value = m.value;
    meas_id = m.id;
  }

  void set_last(Meas m) {
    data_last.flag = m.flag;
    data_last.time = m.time;
    data_last.value = m.value;
  }
};
#pragma pack(pop)

class Chunk;
typedef std::shared_ptr<Chunk> Chunk_Ptr;

class Chunk : public std::enable_shared_from_this<Chunk> {
protected:
  Chunk(ChunkHeader *hdr, uint8_t *buffer, uint32_t _size, const Meas &first_m);
  Chunk(ChunkHeader *hdr, uint8_t *buffer);

public:
  class IChunkReader {
  public:
    virtual Meas readNext() = 0;
    virtual bool is_end() const = 0;
    virtual ~IChunkReader() {}
  };

  using ChunkReader_Ptr = std::shared_ptr<Chunk::IChunkReader>;

  typedef uint8_t *u8vector;

  EXPORT static Chunk_Ptr create(ChunkHeader *hdr, uint8_t *buffer, uint32_t _size,
                                 const Meas &first_m);
  EXPORT static Chunk_Ptr open(ChunkHeader *hdr, uint8_t *buffer);
  EXPORT ~Chunk();

  EXPORT bool append(const Meas &m);
  EXPORT bool isFull() const;
  EXPORT ChunkReader_Ptr getReader();
  EXPORT void close();
  EXPORT uint32_t calcChecksum();
  EXPORT uint32_t getChecksum();
  EXPORT virtual bool checkId(const Id &id);
  EXPORT virtual bool checkChecksum();
  EXPORT bool checkFlag(const Flag &f);

  EXPORT static void updateChecksum(ChunkHeader &hdr, u8vector buff);
  EXPORT static uint32_t calcChecksum(ChunkHeader &hdr, u8vector buff);
  /// return - count of skipped bytes.
  EXPORT static uint32_t compact(ChunkHeader *hdr);
  ChunkHeader *header;
  u8vector _buffer_t;

  compression::ByteBuffer_Ptr bw;
  compression::CopmressedWriter c_writer;
  bool is_owner; // true - dealloc memory for header and buffer.
};

typedef std::list<Chunk_Ptr> ChunksList;
typedef std::map<Id, Chunk_Ptr> IdToChunkMap;
typedef std::map<Id, ChunksList> ChunkMap;
typedef std::unordered_map<Id, Chunk_Ptr> IdToChunkUMap;
}
}
