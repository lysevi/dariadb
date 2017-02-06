#include <libdariadb/interfaces/ichunkcontainer.h>

using namespace dariadb;

IChunkStorage::~IChunkStorage() {}

IChunkContainer::IChunkContainer() {}
IChunkContainer::~IChunkContainer() {}

void IChunkContainer::foreach (const QueryInterval &query, IReadCallback * clb) {
  auto readers = intervalReader(query);
  for (auto kv : readers) {
    kv.second->apply(clb,query);
  }
}
