#include "ichunkcontainer.h"

using namespace dariadb::storage;

void IChunkContainer::foreach (const QueryInterval &query, IReaderClb * clb) {
  auto all_chunkLinks = this->chunksByIterval(query);
  this->readLinks(query, all_chunkLinks, clb);
  all_chunkLinks.clear();
}
