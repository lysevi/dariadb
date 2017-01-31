#include <libdariadb/interfaces/ichunkcontainer.h>

using namespace dariadb::storage;

IChunkStorage::~IChunkStorage() {}

IChunkContainer::IChunkContainer() {}
IChunkContainer::~IChunkContainer() {}

dariadb::Id2Reader IChunkContainer::intervalReader(const QueryInterval &query) {
  auto all_chunkLinks = this->linksByIterval(query);
  return this->intervalReader(query, all_chunkLinks);
}

void IChunkContainer::foreach (const QueryInterval &query, IReaderClb * clb) {
  auto readers = intervalReader(query);
  for (auto kv : readers) {
    kv.second->apply(clb,query);
  }
}
