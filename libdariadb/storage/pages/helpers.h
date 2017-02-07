#pragma once

#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/pages/page.h>
#include <fstream>
#include <map>
#include <tuple>

namespace dariadb {
namespace storage {
namespace PageInner {
struct HdrAndBuffer {
  dariadb::storage::ChunkHeader hdr;
  std::shared_ptr<uint8_t> buffer;
};

std::map<Id, MeasArray> splitById(const MeasArray &ma);

std::list<HdrAndBuffer> compressValues(std::map<Id, MeasArray> &to_compress,
                                       PageFooter &phdr, uint32_t max_chunk_size);

uint64_t writeToFile(FILE *file, FILE *index_file, PageFooter &phdr, IndexFooter &,
                     std::list<HdrAndBuffer> &compressed_results, uint64_t file_size = 0);

IndexReccord init_chunk_index_rec(const ChunkHeader &cheader, IndexFooter *iheader);
}
}
}