#pragma once

#include "../meas.h"
#include "../utils/fs.h"

#include <stx/btree_multimap.h>

#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>

namespace dariadb {
namespace storage {
#pragma pack(push, 1)

struct IndexHeader {
  uint32_t count; // count of values
  uint64_t pos;   // next write pos(bytes)

  dariadb::Time minTime;
  dariadb::Time maxTime;

  uint32_t chunk_per_storage; // max chunks count
  uint32_t chunk_size;        // each chunks size in bytes
  bool is_sorted;             // items sorted by time

  dariadb::Id id_bloom; // bloom filter of Meas.id
};

struct Page_ChunkIndex {
  Time minTime, maxTime;    // min max time of linked chunk
  dariadb::Id meas_id;      // chunk->info->first.id
  dariadb::Flag flag_bloom; // bloom filter of writed meases
  uint64_t chunk_id;        // chunk->id
  bool is_readonly;         // chunk is full?
  uint64_t offset;          // offset in bytes of chunk in page
  bool is_init;             // is init :)
};
#pragma pack(pop)

typedef stx::btree_multimap<dariadb::Time, uint32_t> indexTree;

class IndexFile{
public:
    IndexFile()=delete;
    static IndexFile *create(std::string file_name, uint32_t chunk_per_storage,
                        uint32_t chunk_size);
    static IndexFile *open(std::string file_name, bool read_only = false);
    ~IndexFile();

protected:
    uint8_t *iregion; // index file mapp region

    IndexHeader *iheader;
    Page_ChunkIndex *index;
    indexTree _itree; // needed to sort index reccord on page closing. for fast search
    mutable utils::fs::MappedFile::MapperFile_ptr index_mmap;
    mutable boost::shared_mutex _locker;
};

typedef std::shared_ptr<IndexFile> IndexFile_Ptr;
}
}
