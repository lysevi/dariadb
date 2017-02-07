#pragma once

#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/chunkcontainer.h>
#include <libdariadb/utils/fs.h>

namespace dariadb {
namespace storage {
#pragma pack(push, 1)
struct IndexFooter {
  bool is_sorted;    // items in index file sorted by time
  uint64_t id_bloom; // bloom filter of Meas.id

  Statistic stat;
  uint64_t recs_count;

  uint16_t level;
  IndexFooter() : stat() {
    level = 0;
    recs_count = 0;
    is_sorted = false;
    id_bloom = bloom_empty<Id>();
  }
};

struct IndexReccord {
  size_t meas_id;
  uint64_t chunk_id; // chunk->id
  uint64_t offset;   // offset in bytes of chunk in page
  Statistic stat;
  IndexReccord() : stat() {
    meas_id = 0;
    chunk_id = 0;
    offset = 0;
  }
};
#pragma pack(pop)

class PageIndex;
typedef std::shared_ptr<PageIndex> PageIndex_ptr;
class PageIndex {
public:
  bool readonly;
  std::string filename;
  IndexFooter iheader;

  ~PageIndex();
  static PageIndex_ptr open(const std::string &filename);

  ChunkLinkList get_chunks_links(const dariadb::IdArray &ids,
                                 dariadb::Time from, dariadb::Time to,
                                 dariadb::Flag flag);
  std::vector<IndexReccord> readReccords();
  static IndexFooter readIndexFooter(std::string ifile);

  static std::string index_name_from_page_name(const std::string &page_name) {
    return page_name + "i";
  }
};
}
}
