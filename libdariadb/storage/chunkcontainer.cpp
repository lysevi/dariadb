#include <libdariadb/storage/chunkcontainer.h>

using namespace dariadb;

IChunkStorage::~IChunkStorage() {}

ChunkContainer::ChunkContainer() {}
ChunkContainer::~ChunkContainer() {}

void ChunkContainer::foreach (const QueryInterval &query, IReadCallback * clb) {
  auto readers = intervalReader(query);
  for (auto kv : readers) {
    kv.second->apply(clb,query);
  }
}
