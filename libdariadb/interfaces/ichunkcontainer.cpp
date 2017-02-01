#include <libdariadb/interfaces/ichunkcontainer.h>

using namespace dariadb::storage;

IChunkStorage::~IChunkStorage() {}

IChunkContainer::IChunkContainer() {}
IChunkContainer::~IChunkContainer() {}

void IChunkContainer::foreach (const QueryInterval &query, IReaderClb * clb) {
  auto readers = intervalReader(query);
  for (auto kv : readers) {
    kv.second->apply(clb,query);
  }
}
