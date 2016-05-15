#pragma once
#include "../storage.h"
#include "../utils/fs.h"
#include "../utils/locker.h"
#include "chunk.h"
#include "chunk_container.h"
#include "cursor.h"
#include "mode.h"

#include "stx/btree_multimap.h"
#include <map>
#include <unordered_map>
#include "bloom_filter.h"

namespace dariadb {
namespace storage {
#pragma pack(push, 1)
struct PageHeader {
  uint32_t pos;

  uint32_t count_readers;

  uint64_t addeded_chunks;
  // uint8_t is_overwrite;

  uint32_t chunk_per_storage;
  uint32_t chunk_size;

  bool is_full;

  dariadb::Time minTime;
  dariadb::Time maxTime;
};

struct IndexHeader {
  uint32_t count;
  uint32_t pos;

  dariadb::Time minTime;
  dariadb::Time maxTime;

  uint32_t chunk_per_storage;
  uint32_t chunk_size;
  bool is_sorted;

  dariadb::Id id_bloom;
};

struct Page_ChunkIndex {
  Time minTime, maxTime;
  dariadb::Meas first, last;
  dariadb::Flag flag_bloom;
  bool is_readonly;
  uint64_t offset;
  bool is_init;
};
#pragma pack(pop)

// maxtime => pos index rec in page;
// typedef std::multimap<dariadb::Time, uint32_t> indexTree;
typedef stx::btree_multimap<dariadb::Time, uint32_t> indexTree;
typedef std::map<dariadb::Time, uint32_t> Time2Pos;
typedef std::unordered_map<dariadb::Id, Time2Pos> PageMultiTree;
class Page : public ChunkContainer, public MeasWriter {
  Page() = default;

public:
  static Page *create(std::string file_name, uint64_t sz,
                      uint32_t chunk_per_storage, uint32_t chunk_size);
  static Page *open(std::string file_name);
  static PageHeader readHeader(std::string file_name);
  static IndexHeader readIndexHeader(std::string page_file_name);
  ~Page();
  // PM
  bool add_to_target_chunk(const dariadb::Meas &m);
  /*bool append(const ChunksList &ch) override;*/
  bool is_full() const;
  Cursor_ptr get_chunks(const dariadb::IdArray &ids, dariadb::Time from,
                        dariadb::Time to, dariadb::Flag flag);
  // ChunksList get_open_chunks();
  void dec_reader();
  // ChunkContainer
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  Cursor_ptr chunksByIterval(const QueryInterval &query) override;
  IdToChunkMap chunksBeforeTimePoint(const QueryTimePoint &q) override;
  IdArray getIds() override;

  // Inherited via MeasWriter
  virtual append_result append(const Meas &value) override;
  virtual void flush() override;

private:
  void init_chunk_index_rec(Chunk_Ptr ch, uint8_t *addr);
  void update_chunk_index_rec(const Chunk_Ptr &ch);

public:
  uint8_t *region;  // page  file mapp region
  uint8_t *iregion; // index file mapp region
  PageHeader *header;
  IndexHeader *iheader;

  Page_ChunkIndex *index;
  uint8_t *chunks;

  indexTree _itree;
  PageMultiTree _mtree;
  std::list<uint32_t> _free_poses;

  std::string filename;
protected:
  mutable std::mutex _locker;
  mutable utils::fs::MappedFile::MapperFile_ptr page_mmap;
  mutable utils::fs::MappedFile::MapperFile_ptr index_mmap;
};
}
}
