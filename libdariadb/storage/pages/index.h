#pragma once

#include <libdariadb/interfaces/ichunkcontainer.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/storage/chunk.h>

namespace dariadb {
namespace storage {
#pragma pack(push, 1)
struct IndexHeader {
  uint32_t count; // count of values
  dariadb::Time minTime;
  dariadb::Time maxTime;
  bool is_sorted; // items in index file sorted by time
  bool is_closed;
  bool is_full;
  uint64_t id_bloom;   // bloom filter of Meas.id
  uint64_t flag_bloom; // bloom filter of Meas.flag
};

struct IndexReccord {
  Time minTime, maxTime; // min max time of linked chunk
  size_t flag_bloom;     // bloom filters of writed meases
  size_t meas_id;
  uint64_t chunk_id; // chunk->id
  uint64_t offset;   // offset in bytes of chunk in page
  bool is_init : 1;  // is init :)
};
#pragma pack(pop)

class PageIndex;
typedef std::shared_ptr<PageIndex> PageIndex_ptr;
class PageIndex {
public:
  bool readonly;
  IndexHeader *iheader;
  uint8_t *iregion; // index file mapp region
  IndexReccord *index;

  std::string filename;

  mutable utils::fs::MappedFile::MapperFile_ptr index_mmap;

  ~PageIndex();
  static PageIndex_ptr create(const std::string &filename, uint64_t size);
  static PageIndex_ptr open(const std::string &filename, bool read_only);

  ChunkLinkList get_chunks_links(const dariadb::IdArray &ids, dariadb::Time from,
                                 dariadb::Time to, dariadb::Flag flag);

  static IndexHeader readIndexHeader(std::string ifile);

  static std::string index_name_from_page_name(const std::string &page_name) {
    return page_name + "i";
  }
};
}
}
