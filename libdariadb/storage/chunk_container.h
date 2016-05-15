#pragma once

#include "../meas.h"
#include "chunk.h"
#include "cursor.h"
#include "mode.h"
#include "query_param.h"
namespace dariadb {
namespace storage {

class ChunkWriter {
public:
  virtual bool append(const Chunk_Ptr &c) = 0;
  virtual bool append(const ChunksList &lst) = 0;
  virtual ~ChunkWriter(){}
};

class ChunkContainer {
public:
  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) = 0;
  virtual Cursor_ptr chunksByIterval(const QueryInterval &query) = 0;
  virtual IdToChunkMap chunksBeforeTimePoint(const QueryTimePoint &q) = 0;
  virtual IdArray getIds() = 0;
  virtual ~ChunkContainer();
};
}
}
