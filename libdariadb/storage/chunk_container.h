#pragma once

#include "../meas.h"
#include "chunk.h"
#include "cursor.h"
#include "query_param.h"

namespace dariadb {
namespace storage {

class ChunkWriter {
public:
  virtual bool append(const Chunk_Ptr &c) = 0;
  virtual bool append(const ChunksList &lst) = 0;
  virtual ~ChunkWriter() {}
};

struct ChunkLink {
  uint64_t id;
  dariadb::Id first_id;
  dariadb::Time maxTime;
  std::string page_name;
  uint32_t pos;
};

using ChunkLinkList = std::list<ChunkLink>;

class ChunkContainer {
public:
  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) = 0;
  virtual ChunkLinkList chunksByIterval(const QueryInterval &query) = 0;
  virtual Meas::Id2Meas valuesBeforeTimePoint(const QueryTimePoint &q) = 0;
  virtual Cursor_ptr readLinks(const ChunkLinkList &links) = 0;
  virtual ~ChunkContainer();
};
}
}
