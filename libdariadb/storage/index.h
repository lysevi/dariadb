#pragma once
#include "../storage.h"
#include "../utils/fs.h"
#include "chunk.h"

#include <boost/thread/shared_mutex.hpp>
namespace dariadb {
namespace storage {
#pragma pack(push, 1)
struct IndexHeader {
  uint32_t count; // count of values
  // uint64_t pos;   // next write pos(bytes)

  dariadb::Time minTime;
  dariadb::Time maxTime;

  uint32_t chunk_per_storage; // max chunks count
  uint32_t chunk_size;        // each chunks size in bytes
  bool is_sorted;             // items in index file sorted by time
  bool is_closed;
  dariadb::Id id_bloom; // bloom filter of Meas.id
};

struct IndexReccord {
  Time minTime, maxTime; // min max time of linked chunk
  // dariadb::Id meas_id;      // chunk->info->first.id
  dariadb::Flag flag_bloom; // bloom filters of writed meases
  dariadb::Id id_bloom;
  uint64_t chunk_id; // chunk->id
  // bool is_readonly;         // chunk is full?
  uint64_t offset; // offset in bytes of chunk in page
  bool is_init;    // is init :)
};
#pragma pack(pop)

const size_t INDEX_FLUSH_PERIOD = 1000;

// maxtime => pos index rec in page;
typedef std::multimap<dariadb::Time, uint32_t> indexTree;
// typedef stx::btree_multimap<dariadb::Time, uint32_t> indexTree;

class PageIndex;
typedef std::shared_ptr<PageIndex> PageIndex_ptr;
class PageIndex {
public:
  bool readonly;
  IndexHeader *iheader;
  uint8_t *iregion; // index file mapp region
  IndexReccord *index;

  indexTree _itree; // needed to sort index reccord on page closing. for fast search
  std::string filename;
  mutable boost::shared_mutex _locker;
  mutable utils::fs::MappedFile::MapperFile_ptr index_mmap;

  ~PageIndex();
  static PageIndex_ptr create(const std::string &filename, uint64_t size,
                              uint32_t chunk_per_storage, uint32_t chunk_size);
  static PageIndex_ptr open(const std::string &filename, bool read_only);

  void update_index_info(IndexReccord *cur_index, const Chunk_Ptr &ptr, const Meas &m,
                         uint32_t pos);

  ChunkLinkList get_chunks_links(const dariadb::IdArray &ids, dariadb::Time from,
                                 dariadb::Time to, dariadb::Flag flag);

  static std::string index_name_from_page_name(const std::string &page_name) {
    return page_name + "i";
  }
};
}
}
