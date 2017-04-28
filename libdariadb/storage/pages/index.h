#pragma once

#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/chunkcontainer.h>
#include <libdariadb/storage/magic.h>
#include <libdariadb/utils/fs.h>

namespace dariadb {
namespace storage {
#pragma pack(push, 1)
struct IndexFooter {
  uint64_t magic_number;
  bool is_sorted; // items in index file sorted by time
  Id target_id;   // measurement id

  Statistic stat;
  uint64_t recs_count;

  uint16_t level;

  uint64_t parent_offset; // offset of previous footer. need for futer ideas. not used
                          // currently
  uint8_t ftr_end;
  IndexFooter() : stat() {
    magic_number = MAGIC_NUMBER_DARIADB;
    level = 0;
    recs_count = 0;
    is_sorted = false;
    target_id = Id();
    parent_offset = uint64_t();
    ftr_end = MAGIC_NUMBER_INDEXFTR;
  }

  bool check() {
    if (parent_offset != uint64_t()) {
      return false;
    }
    if (magic_number != MAGIC_NUMBER_DARIADB) {
      return false;
    }
    if (ftr_end != MAGIC_NUMBER_INDEXFTR) {
      return false;
    }
    return true;
  }
};

struct IndexReccord {
  size_t target_id;  // measurement id
  uint64_t chunk_id; // chunk->id
  uint64_t offset;   // offset in bytes of chunk in page
  Statistic stat;
  IndexReccord() : stat() {
    target_id = 0;
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

  ChunkLinkList get_chunks_links(const dariadb::IdArray &ids, dariadb::Time from,
                                 dariadb::Time to, dariadb::Flag flag);
  std::vector<IndexReccord> readReccords();
  static IndexFooter readIndexFooter(std::string ifile);

  static std::string index_name_from_page_name(const std::string &page_name) {
    return page_name + "i";
  }
};
}
} // namespace dariadb
