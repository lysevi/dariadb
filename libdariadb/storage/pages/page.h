#pragma once
#include <libdariadb/interfaces/ichunkcontainer.h>
#include <libdariadb/interfaces/imeaswriter.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/pages/index.h>
#include <libdariadb/utils/fs.h>

namespace dariadb {
namespace storage {

const std::string PAGE_FILE_EXT = ".page"; // cola-file extension

#pragma pack(push, 1)
struct PageHeader {
  // uint64_t write_offset;      // next write pos (bytes)
  uint32_t addeded_chunks; // total count of chunks in page.
  uint64_t filesize;
  Statistic stat;
  uint64_t max_chunk_id; // max(chunk->id)
  PageHeader() : stat() {
    addeded_chunks = 0;
    filesize = 0;
    max_chunk_id = 0;
  }
};
#pragma pack(pop)

class Page;
typedef std::shared_ptr<Page> Page_Ptr;

class Page : public IChunkContainer {
  Page() = default;

public:
  /// called by Dropper from Wal level.
  EXPORT static Page_Ptr create(const std::string &file_name, uint64_t chunk_id,
                                uint32_t max_chunk_size, const MeasArray &ma);
  /// used for compaction many pages to one
  EXPORT static Page_Ptr create(const std::string &file_name, uint64_t chunk_id,
                                uint32_t max_chunk_size,
                                const std::list<std::string> &pages_full_paths);
  /// called by dropper from MemoryStorage.
  EXPORT static Page_Ptr create(const std::string &file_name, uint64_t chunk_id,
                                const std::vector<Chunk *> &a, size_t count);

  EXPORT static Page_Ptr open(std::string file_name);

  EXPORT static PageHeader readHeader(std::string file_name);
  EXPORT static IndexHeader readIndexHeader(std::string page_file_name);

  EXPORT static uint64_t index_file_size(uint32_t chunk_per_storage);
  EXPORT static void restoreIndexFile(const std::string &file_name);

  EXPORT ~Page();

  // ChunkContainer
  EXPORT bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                         dariadb::Time *maxResult) override;
  EXPORT Id2Meas valuesBeforeTimePoint(const QueryTimePoint &q) override;
  EXPORT Id2Reader intervalReader(const QueryInterval &query) override;
  EXPORT void appendChunks(const std::vector<Chunk *> &a,
                           size_t count) override;

  EXPORT Id2MinMax loadMinMax();
  EXPORT Id2Reader intervalReader(const QueryInterval &query,
                                  const ChunkLinkList &links);

  bool checksum(); // return false if bad checksum.
private:
  void update_index_recs(const PageHeader &phdr);

  static Page_Ptr make_page(const std::string &file_name,
                            const PageHeader &phdr);
  Chunk_Ptr readChunkByOffset(FILE *page_io, int offset);

  ChunkLinkList linksByIterval(const QueryInterval &qi);

public:
  PageHeader header;
  std::string filename;

protected:
  PageIndex_ptr _index;
};
}
}
