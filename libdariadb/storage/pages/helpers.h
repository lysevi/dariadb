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

/// @file_name - result file name
/// @phdr - page header.
/// compressed_results - tuples ChunkHeader + ChunkBuffer
/// @file_size - if file_name exists, start_offset contains file size in bytes:
/// header + chunks.
/// @add_header_size_to_result - add to result size of PageHeader. needed if you
/// dont append many time values.
/// @result - page file size in bytes
uint64_t writeToFile(FILE* file, PageHeader &phdr,
                     std::list<HdrAndBuffer> &compressed_results, uint64_t file_size = 0);
}
}
}