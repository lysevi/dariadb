#pragma once
#include <libdariadb/interfaces/ichunkcontainer.h>
#include <libdariadb/interfaces/imeaswriter.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/pages/index.h>
#include <libdariadb/st_exports.h>

namespace dariadb {
namespace storage {

const std::string PAGE_FILE_EXT = ".page"; // cola-file extension

#pragma pack(push, 1)
struct PageHeader {
  // uint64_t write_offset;      // next write pos (bytes)
  uint32_t addeded_chunks; // total count of chunks in page.
  uint64_t filesize;
  Time minTime;              // minimal stored time
  Time maxTime;              // maximum stored time
  uint64_t max_chunk_id;     // max(chunk->id)
};
#pragma pack(pop)

class Page : public IChunkContainer {
  Page() = default;

public:
	///called by Dropper from Aof level.
  EXPORT static Page *create(const std::string &file_name, uint64_t chunk_id,
                      uint32_t max_chunk_size, const MeasArray &ma);
  /// used for compaction many pages to one
  EXPORT static Page *create(const std::string &file_name, uint64_t chunk_id,
	  uint32_t max_chunk_size, const std::list<std::string>& pages_full_paths);
  /// called by dropper from MemoryStorage.
  EXPORT static Page *create(const std::string &file_name, uint64_t chunk_id,
	  const std::vector<Chunk*>& a, size_t count);

  EXPORT static Page *open(std::string file_name);
  
  EXPORT static PageHeader readHeader(std::string file_name);
  EXPORT static IndexHeader readIndexHeader(std::string page_file_name);
  
  EXPORT static uint64_t index_file_size(uint32_t chunk_per_storage);
  EXPORT static void restoreIndexFile(const std::string&file_name);
  
  EXPORT ~Page();

  // ChunkContainer
  EXPORT bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  EXPORT ChunkLinkList chunksByIterval(const QueryInterval &query) override;
  EXPORT Id2Meas valuesBeforeTimePoint(const QueryTimePoint &q) override;
  EXPORT void readLinks(const QueryInterval &query, const ChunkLinkList &links,
                 IReaderClb *clbk) override;

  EXPORT void appendChunks(const std::vector<Chunk*>&a, size_t count)override;

  EXPORT Id2MinMax loadMinMax();
  bool checksum();//return false if bad checksum.
private:
  void update_index_recs(const PageHeader &phdr);


  void check_page_struct();
  static Page* make_page(const std::string&file_name, const PageHeader&phdr);
  Chunk_Ptr readChunkByOffset(FILE* page_io, int offset);
public:
  PageHeader header;
  std::string filename;
protected:
  PageIndex_ptr _index;
};

typedef std::shared_ptr<Page> Page_Ptr;
}
}
