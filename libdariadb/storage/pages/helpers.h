#pragma once

#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/pages/page.h>
#include <map>
#include <tuple>
#include <fstream>

namespace dariadb {
namespace storage {
namespace PageInner {
struct HdrAndBuffer {
  dariadb::storage::ChunkHeader hdr;
  std::shared_ptr<uint8_t> buffer;
};

dariadb::storage::PageHeader emptyPageHeader(uint64_t chunk_id);

std::map<Id, MeasArray> splitById(const MeasArray &ma);

std::list<HdrAndBuffer> compressValues(std::map<Id, MeasArray> &to_compress,
                                       PageHeader &phdr, uint32_t max_chunk_size);

uint64_t writeToFile(FILE* file, FILE* index_file, PageHeader &phdr, IndexHeader&,
                     std::list<HdrAndBuffer> &compressed_results, uint64_t file_size = 0);

IndexReccord  init_chunk_index_rec(const ChunkHeader& cheader, IndexHeader* iheader);
}
}
}