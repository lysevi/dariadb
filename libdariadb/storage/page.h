#pragma once
#include <libdariadb/interfaces/ichunkcontainer.h>
#include <libdariadb/interfaces/imeaswriter.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/index.h>

namespace dariadb {
namespace storage {

const std::string PAGE_FILE_EXT = ".page"; // cola-file extension

#pragma pack(push, 1)
struct PageHeader {
  // uint64_t write_offset;      // next write pos (bytes)
  uint32_t addeded_chunks; // total count of chunks in page.
  uint32_t removed_chunks; // total chunks marked as not init in rollbacks or fsck.
  uint64_t filesize;
  bool is_full : 1;          // is full :)
  bool is_closed : 1;        // is correctly closed.
  bool is_open_to_write : 1; // true if oppened to write.
  Time minTime;              // minimal stored time
  Time maxTime;              // maximum stored time
  uint64_t max_chunk_id;     // max(chunk->id)
};
#pragma pack(pop)

class Page : public IChunkContainer {
  Page() = default;

public:
  static Page *create(const std::string &file_name, uint64_t chunk_id,
                      uint32_t max_chunk_size, const MeasArray &ma);
  static Page *open(std::string file_name, bool read_only = false);
  static PageHeader readHeader(std::string file_name);
  static IndexHeader readIndexHeader(std::string page_file_name);
  static uint64_t index_file_size(uint32_t chunk_per_storage);
  static void restoreIndexFile(const std::string&file_name);
  ~Page();
  void fsck();
  bool is_full() const;

  // ChunkContainer
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  ChunkLinkList chunksByIterval(const QueryInterval &query) override;
  Id2Meas valuesBeforeTimePoint(const QueryTimePoint &q) override;
  void readLinks(const QueryInterval &query, const ChunkLinkList &links,
                 IReaderClb *clb) override;

  void flush();

  void mark_as_non_init(Chunk_Ptr &ch);
private:
  void update_index_recs();
  void init_chunk_index_rec(Chunk_Ptr ch, uint32_t pos_index);
  struct ChunkWithIndex {
    Chunk_Ptr ch;        /// ptr to chunk in page
    IndexReccord *index; /// ptr to index reccord
    uint32_t pos;        /// position number of 'index' field in index file.
  };
  /// cache of openned chunks. before search chunk in page, we search in cache.
  ChunkWithIndex _openned_chunk;

  void check_page_struct();

public:
  uint8_t *region; // page  file mapp region
  PageHeader *header;
  uint8_t *chunks;

  std::string filename;
  bool readonly;

protected:
  PageIndex_ptr _index;
  mutable utils::fs::MappedFile::MapperFile_ptr page_mmap;
};

typedef std::shared_ptr<Page> Page_Ptr;
}
}
