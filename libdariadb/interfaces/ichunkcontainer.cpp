#include <libdariadb/interfaces/ichunkcontainer.h>

using namespace dariadb::storage;

IChunkStorage::~IChunkStorage() {}

IChunkContainer::IChunkContainer() {}
IChunkContainer::~IChunkContainer() {}

void IChunkContainer::foreach (const QueryInterval &query, IReaderClb * clb) {
  auto all_chunkLinks = this->chunksByIterval(query);
  this->readLinks(query, all_chunkLinks, clb);
  all_chunkLinks.clear();
}
