#pragma once
#include "../storage.h"
#include "../utils/fs.h"
#include "chunk.h"
#include "chunk_container.h"
#include "index.h"

namespace dariadb {
namespace storage {
#pragma pack(push, 1)
struct PageHeader {
  uint64_t pos;               // next write pos (bytes)
  uint32_t count_readers;     // readers count. on close must be zero.
  uint64_t addeded_chunks;    // total count of chunks in page.
  uint64_t removed_chunks;    // total chunks marked as not init in rollbacks or fsck.
  uint32_t chunk_per_storage; // max chunks count
  uint32_t chunk_size;        // each chunks size in bytes
  bool is_full : 1;           // is full :)
  bool is_closed : 1;
  bool is_open_to_write : 1; // true if oppened to write.
  dariadb::Time minTime;
  dariadb::Time maxTime;
  uint64_t max_chunk_id; // max(chunk->id)

  uint64_t transaction;
  bool _under_transaction;
};
#pragma pack(pop)

const size_t PAGE_FLUSH_PERIOD = 1000;

class Page : public ChunkContainer, public MeasWriter {
  Page() = default;

public:
  static Page *create(std::string file_name, uint64_t sz, uint32_t chunk_per_storage,
                      uint32_t chunk_size);
  static Page *open(std::string file_name, bool read_only = false);
  static PageHeader readHeader(std::string file_name);
  static IndexHeader readIndexHeader(std::string page_file_name);
  ~Page();
  void fsck();
  // PM
  bool add_to_target_chunk(const dariadb::Meas &m);
  /*bool append(const ChunksList &ch) override;*/
  bool is_full() const;

  // ChunksList get_open_chunks();
  void dec_reader();
  // ChunkContainer
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  ChunkLinkList chunksByIterval(const QueryInterval &query) override;
  Meas::Id2Meas valuesBeforeTimePoint(const QueryTimePoint &q) override;
  void readLinks(const QueryInterval &query, const ChunkLinkList &links,
                 ReaderClb *clb) override;

  // Inherited via MeasWriter
  virtual append_result append(const Meas &value) override;
  virtual void flush() override;

  void begin_transaction(uint64_t num);
  void commit_transaction(uint64_t num);
  void rollback_transaction(uint64_t num);

  std::list<Chunk_Ptr> get_not_full_chunks(); //list of not full chunks
  void mark_as_non_init(Chunk_Ptr&ch);
private:
  void flush_current_chunk();
  void init_chunk_index_rec(Chunk_Ptr ch);
  void close_corrent_chunk();
  struct ChunkWithIndex {
    Chunk_Ptr ch;        /// ptr to chunk in page
    IndexReccord *index; /// ptr to index reccord
    uint32_t pos;        /// position number of 'index' field in index file.
  };
  /// cache of openned chunks. before search chunk in page, we search in cache.
  ChunkWithIndex _openned_chunk;

public:
  uint8_t *region; // page  file mapp region

  PageHeader *header;

  uint8_t *chunks;

  std::list<uint32_t> _free_poses;

  std::string filename;
  bool readonly;
  PageIndex_ptr _index;

protected:
  mutable std::mutex _locker;
  mutable utils::fs::MappedFile::MapperFile_ptr page_mmap;
};

typedef std::shared_ptr<Page> Page_Ptr;
}
}
