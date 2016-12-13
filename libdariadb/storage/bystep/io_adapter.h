#pragma once

#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/st_exports.h>

#include <list>

namespace dariadb {
namespace storage {

using ChunksList = std::list<Chunk_Ptr>;
class IOAdapter {
public:
  EXPORT IOAdapter(const std::string &fname);
  EXPORT ~IOAdapter();
  EXPORT void stop();
  EXPORT void append(const Chunk_Ptr&ch);
  EXPORT ChunksList readInterval(const QueryInterval &query);
  EXPORT IdToChunkMap readTimePoint(const QueryTimePoint &query);
  EXPORT void replace(const Chunk_Ptr&ch);
private:
  class Private;
  std::unique_ptr<Private> _impl;
};
}
}