#pragma once
#include "../storage.h"
#include "../utils/fs.h"
#include "../utils/locker.h"
#include "chunk.h"
#include "chunk_container.h"
#include "cursor.h"
#include "bloom_filter.h"

#include "stx/btree_multimap.h"
#include <map>
#include <unordered_map>

#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>

namespace dariadb {
namespace storage {
#pragma pack(push, 1)
struct PageHeader {
  uint64_t pos;           //next write pos (bytes)
  uint32_t count_readers; //readers count. on close must be zero.
  uint64_t addeded_chunks; //total count of chunks in page
  uint32_t chunk_per_storage; //max chunks count
  uint32_t chunk_size;        //each chunks size in bytes
  bool is_full;               //is full :)
  dariadb::Time minTime;
  dariadb::Time maxTime;
  uint64_t max_chunk_id;      //max(chunk->id)
};

struct IndexHeader {
  uint32_t count;           //count of values
  uint64_t pos;             //next write pos(bytes)

  dariadb::Time minTime;
  dariadb::Time maxTime;

  uint32_t chunk_per_storage; //max chunks count
  uint32_t chunk_size;        //each chunks size in bytes
  bool is_sorted;             //items sorted by time

  dariadb::Id id_bloom;       //bloom filter of Meas.id
};

struct Page_ChunkIndex {
  Time minTime, maxTime;    //min max time of linked chunk
  dariadb::Id   meas_id;    //chunk->info->first.id
  dariadb::Flag flag_bloom; //bloom filter of writed meases
  uint64_t chunk_id;        //chunk->id
  bool is_readonly;         //chunk is full?
  uint64_t offset;          //offset in bytes of chunk in page
  bool is_init;             //is init :)
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
  static Page *open(std::string file_name, bool read_only=false);
  static PageHeader readHeader(std::string file_name);
  static IndexHeader readIndexHeader(std::string page_file_name);
  ~Page();
  // PM
  bool add_to_target_chunk(const dariadb::Meas &m);
  /*bool append(const ChunksList &ch) override;*/
  bool is_full() const;
  ChunkLinkList get_chunks_links(const dariadb::IdArray &ids, dariadb::Time from,
                        dariadb::Time to, dariadb::Flag flag);
  // ChunksList get_open_chunks();
  void dec_reader();
  // ChunkContainer
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  ChunkLinkList chunksByIterval(const QueryInterval &query) override;
  ChunkLinkList chunksBeforeTimePoint(const QueryTimePoint &q) override;
  Cursor_ptr  readLinks(const ChunkLinkList&links) override;
  IdArray getIds() override;

  // Inherited via MeasWriter
  virtual append_result append(const Meas &value) override;
  virtual void flush() override;

private:
  void init_chunk_index_rec(Chunk_Ptr ch, uint8_t *addr);
  void update_chunk_index_rec(const Chunk_Ptr &ptr, const Meas&m);
  void update_index_info(Page_ChunkIndex*cur_index, const uint32_t pos, const Chunk_Ptr &ptr, const Meas&m);
  struct ChunkWithIndex {
	  Chunk_Ptr ch;           ///ptr to chunk in page
	  Page_ChunkIndex*index;  ///ptr to index reccord
	  uint32_t pos;           ///position number of 'index' field in index file.
  };
  ///cache of openned chunks. before search chunk in page, we search in cache.
  std::map<dariadb::Id, ChunkWithIndex> _openned_chunks;
public:
  uint8_t *region;  // page  file mapp region
  uint8_t *iregion; // index file mapp region
  PageHeader *header;
  IndexHeader *iheader;

  Page_ChunkIndex *index;
  uint8_t *chunks;

  indexTree _itree; //needed to sort index reccord on page closing. for fast search
  PageMultiTree _mtree;
  std::list<uint32_t> _free_poses;

  std::string filename;
  bool readonly;
protected:
  mutable boost::shared_mutex _locker;
  mutable utils::fs::MappedFile::MapperFile_ptr page_mmap;
  mutable utils::fs::MappedFile::MapperFile_ptr index_mmap;
};

typedef std::shared_ptr<Page> Page_Ptr;
}
}
