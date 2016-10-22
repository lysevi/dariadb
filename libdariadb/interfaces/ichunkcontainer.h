#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/storage/query_param.h>
#include <libdariadb/interfaces/icallbacks.h>
#include <libdariadb/st_exports.h>
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

class IChunkContainer {
public:
  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) = 0;
  virtual ChunkLinkList chunksByIterval(const QueryInterval &query) = 0;
  virtual Id2Meas valuesBeforeTimePoint(const QueryTimePoint &q) = 0;
  virtual void readLinks(const QueryInterval &query, const ChunkLinkList &links,
                         IReaderClb *clb) = 0;
  EXPORT virtual void foreach (const QueryInterval &query, IReaderClb * clb);
  virtual ~IChunkContainer(){}
};
}
}
