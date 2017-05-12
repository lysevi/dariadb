#ifdef MSVC
#define _CRT_SECURE_NO_WARNINGS // for fopen
#endif
#include <libdariadb/meas.h>
#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/pages/helpers.h>
#include <libdariadb/storage/pages/page.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <algorithm>

#include <cstring>
#include <fstream>

namespace dariadb {
namespace storage {
namespace PageInner {

bool have_overlap(const std::vector<ChunkLink> &links) {
  for (size_t i = 0; i < links.size(); ++i) {
    for (size_t j = 0; j < links.size(); ++j) {
      if (i == j) {
        continue;
      }
      auto from1 = links[i].minTime;
      auto from2 = links[j].minTime;
      auto to1 = links[i].maxTime;
      auto to2 = links[j].maxTime;

      if (utils::intervalsIntersection(from1, to1, from2, to2)) {
        return true;
      }
    }
  }
  return false;
}

std::shared_ptr<std::list<HdrAndBuffer>>
compressValues(const MeasArray &to_compress, PageFooter &phdr, uint32_t max_chunk_size) {
  using namespace dariadb::utils::async;
  auto results = std::make_shared<std::list<HdrAndBuffer>>();

  utils::async::AsyncTask at = [&results, &phdr, max_chunk_size,
                                &to_compress](const utils::async::ThreadInfo &ti) {
    using namespace dariadb::utils::async;
    TKIND_CHECK(dariadb::utils::async::THREAD_KINDS::COMMON, ti.kind);
    auto begin = to_compress.cbegin();
    auto end = to_compress.cend();
    auto it = begin;
    while (it != end) {
      ChunkHeader hdr;
      boost::shared_array<uint8_t> buffer_ptr{new uint8_t[max_chunk_size]};
      memset(buffer_ptr.get(), 0, max_chunk_size);
      auto ch = Chunk::create(&hdr, buffer_ptr.get(), max_chunk_size, *it);
      ++it;
      while (it != end) {
        if (!ch->append(*it)) {
          break;
        }
        ++it;
      }
      ch->close();

      phdr.max_chunk_id++;

      ch->header->id = phdr.max_chunk_id;

      phdr.stat.update(ch->header->stat);

      HdrAndBuffer subres;
      subres.hdr = hdr;
      subres.buffer = buffer_ptr;

      results->push_back(subres);
    }
    return false;
  };
  auto cur_async = ThreadManager::instance()->post(THREAD_KINDS::COMMON, AT(at));
  cur_async->wait();
  return results;
}

uint64_t writeToFile(FILE *file, FILE *index_file, PageFooter &phdr, IndexFooter &ihdr,
                     std::list<HdrAndBuffer> &compressed_results, uint64_t file_size) {

  using namespace dariadb::utils::async;
  uint64_t page_size = 0;

  uint64_t offset = file_size;
  std::vector<IndexReccord> ireccords;
  ireccords.resize(compressed_results.size());
  size_t pos = 0;
  for (auto hb : compressed_results) {
    ChunkHeader chunk_header = hb.hdr;
    auto chunk_buffer_ptr = hb.buffer;

    phdr.addeded_chunks++;
    phdr.stat.update(chunk_header.stat);
    auto skip_count = Chunk::compact(&chunk_header);
    chunk_header.offset_in_page = offset;
    // update checksum;
    Chunk::updateChecksum(chunk_header, chunk_buffer_ptr.get() + skip_count);
#ifdef DOUBLE_CHECKS
    {
      auto ch = Chunk::open(&chunk_header, chunk_buffer_ptr.get() + skip_count);
      ENSURE(ch->checkChecksum());
      auto rdr = ch->getReader();
      while (!rdr->is_end()) {
        rdr->readNext();
      }
      ch->close();
    }
#endif
    std::fwrite(&(chunk_header), sizeof(ChunkHeader), 1, file);
    std::fwrite(chunk_buffer_ptr.get() + skip_count, sizeof(uint8_t), chunk_header.size,
                file);

    offset += sizeof(ChunkHeader) + chunk_header.size;

    auto index_reccord = init_chunk_index_rec(chunk_header, &ihdr);
    ireccords[pos] = index_reccord;
    pos++;
  }
  std::fwrite(ireccords.data(), sizeof(IndexReccord), ireccords.size(), index_file);
  page_size = offset;
  ihdr.stat = phdr.stat;
  ENSURE(memcmp(&phdr.stat, &ihdr.stat, sizeof(Statistic)) == 0);
  return page_size;
}

IndexReccord init_chunk_index_rec(const ChunkHeader &cheader, IndexFooter *iheader) {
  IndexReccord cur_index;

  cur_index.chunk_id = cheader.id;
  cur_index.offset = cheader.offset_in_page;

  iheader->target_id = cheader.meas_id;
  iheader->recs_count++;
  cur_index.target_id = cheader.meas_id;
  cur_index.stat = cheader.stat;
  ENSURE(cur_index.stat.minTime <= cur_index.stat.maxTime);
  ENSURE(cur_index.stat.minValue <= cur_index.stat.maxValue);
  return cur_index;
}
} // namespace PageInner
} // namespace storage
} // namespace dariadb
