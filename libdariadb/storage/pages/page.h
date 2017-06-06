#pragma once
#include <libdariadb/interfaces/icompactioncontroller.h>
#include <libdariadb/interfaces/imeaswriter.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/chunkcontainer.h>
#include <libdariadb/storage/magic.h>
#include <libdariadb/storage/pages/index.h>
#include <libdariadb/utils/fs.h>

namespace dariadb {
namespace storage {

const std::string PAGE_FILE_EXT = ".page"; // cola-file extension
const uint16_t MIN_LEVEL = 0;
const uint16_t MAX_LEVEL = std::numeric_limits<uint16_t>::max();
#pragma pack(push, 1)
struct PageFooter {
  uint64_t magic_dariadb;
  uint32_t addeded_chunks; // total count of chunks in page.
  uint64_t filesize;
  Statistic stat;
  uint64_t max_chunk_id; // max(chunk->id)
  uint16_t level;
  uint64_t parent_offset; // offset of previous footer. need for futer ideas. not used
                          // currently
  uint8_t ftr_end;
  PageFooter(uint16_t lvl, uint64_t chunk_id) : stat() {
    magic_dariadb = MAGIC_NUMBER_DARIADB;
    level = lvl;
    max_chunk_id = chunk_id;
    addeded_chunks = 0;
    filesize = 0;
    max_chunk_id = 0;
    parent_offset = uint64_t();
    ftr_end = MAGIC_NUMBER_PAGEFTR;
  }

  bool check() {
    if (parent_offset != uint64_t()) {
      return false;
    }
    if (magic_dariadb != MAGIC_NUMBER_DARIADB) {
      return false;
    }
    if (ftr_end != MAGIC_NUMBER_PAGEFTR) {
      return false;
    }
    return true;
  }
};
#pragma pack(pop)

class Page;
typedef std::shared_ptr<Page> Page_Ptr;
using on_create_complete_callback = std::function<void(const Page_Ptr &)>;
class Page final : public ChunkContainer {
  Page() = delete;

public:
  /// called by Dropper from Wal level.
  EXPORT static void create(const std::string &file_name, uint16_t lvl, uint64_t chunk_id,
                            uint32_t max_chunk_size, const MeasArray &ma,
                            on_create_complete_callback on_complete);
  /// called by dropper from MemoryStorage.
  EXPORT static Page_Ptr create(const std::string &file_name, uint16_t lvl,
	  uint64_t chunk_id, const std::vector<Chunk *> &a,
	  size_t count);
  EXPORT Page(const PageFooter &footer, std::string fname);
  /**
  used for repack many pages to one
  file_name - output page filename
  lvl - output page level
  chunk_id - max chunk id in pagemanager
  max_chunk_size - maximum chunk size
  pages_full_paths - input pages.
  logic - if set, to control repacking.
  */
  EXPORT static Page_Ptr repackTo(const std::string &file_name, uint16_t lvl,
                                  uint64_t chunk_id, uint32_t max_chunk_size,
                                  const std::list<std::string> &pages_full_paths,
                                  ICompactionController *logic);
  
  EXPORT static Page_Ptr open(const std::string &file_name);
  EXPORT static PageFooter readFooter(std::string file_name);
  EXPORT static IndexFooter readIndexFooter(std::string page_file_name);

  EXPORT static void restoreIndexFile(const std::string &file_name);

  EXPORT ~Page();

  // ChunkContainer
  EXPORT bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                         dariadb::Time *maxResult) override;
  EXPORT Id2Meas valuesBeforeTimePoint(const QueryTimePoint &q) override;
  EXPORT Id2Cursor intervalReader(const QueryInterval &query) override;
  EXPORT void appendChunks(const std::vector<Chunk *> &a) override;

  EXPORT Id2MinMax_Ptr loadMinMax();
  EXPORT Id2Cursor intervalReader(const QueryInterval &query, const ChunkLinkList &links);
  EXPORT Statistic stat(const Id id, Time from, Time to);
  EXPORT bool checksum(); // return false if bad checksum.

  // callback - return true for break iteration.
  EXPORT void apply_to_chunks(const ChunkLinkList &links,
                              std::function<bool(const Chunk_Ptr &)> callback);

  EXPORT static Page_Ptr open(const std::string &file_name, const PageFooter &phdr);

private:
  void update_index_recs(const PageFooter &phdr);

  static Chunk_Ptr readChunkByOffset(FILE *page_io, int offset);

  ChunkLinkList linksByIterval(const QueryInterval &qi);

public:
  PageFooter footer;
  std::string filename;

protected:
  PageIndex_ptr _index;
};
}
}
