#pragma once

#include <libdariadb/interfaces/icallbacks.h>
#include <libdariadb/interfaces/ireader.h>
#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/query_param.h>
namespace dariadb {
namespace storage {

struct ChunkLink {
  uint64_t id;
  uint64_t meas_id;
  dariadb::Time minTime;
  dariadb::Time maxTime;
  std::string page_name;
  uint64_t index_rec_number;
};

using ChunkLinkList = std::list<ChunkLink>;

class IChunkStorage {
public:
  virtual void appendChunks(const std::vector<Chunk *> &a, size_t count) = 0;
  EXPORT ~IChunkStorage();
};

class IChunkContainer : public IChunkStorage {
public:
  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) = 0;
  virtual ChunkLinkList linksByIterval(const QueryInterval &query) = 0;
  virtual Id2Meas valuesBeforeTimePoint(const QueryTimePoint &q) = 0;
  virtual Id2Reader intervalReader(const QueryInterval &query,
                                   const ChunkLinkList &links) = 0;

  EXPORT virtual Id2Reader intervalReader(const QueryInterval &query);
  EXPORT virtual void foreach (const QueryInterval &query, IReaderClb * clb);
  EXPORT IChunkContainer();
  EXPORT virtual ~IChunkContainer();
};
}
}
