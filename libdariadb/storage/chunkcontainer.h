#pragma once

#include <libdariadb/interfaces/icallbacks.h>
#include <libdariadb/interfaces/icursor.h>
#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/query_param.h>

namespace dariadb {

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
  virtual void appendChunks(const std::vector<storage::Chunk *> &a, size_t count) = 0;
  EXPORT ~IChunkStorage();
};

class ChunkContainer : public IChunkStorage {
public:
  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) = 0;
  virtual Id2Meas valuesBeforeTimePoint(const QueryTimePoint &q) = 0;
  virtual Id2Cursor intervalReader(const QueryInterval &query) = 0;
  virtual Statistic stat(const Id id, Time from, Time to) = 0;
  EXPORT virtual void foreach (const QueryInterval &query, IReadCallback * clb);
  EXPORT ChunkContainer();
  EXPORT virtual ~ChunkContainer();
};
}

