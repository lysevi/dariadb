#ifdef MSVC
#define _CRT_SECURE_NO_WARNINGS // for fopen
#endif
#include <algorithm>
#include <libdariadb/meas.h>
#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/pages/helpers.h>
#include <libdariadb/storage/pages/page.h>
#include <libdariadb/utils/async/thread_manager.h>

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

std::map<Id, MeasArray> splitById(const MeasArray &ma) {
  dariadb::IdSet dropped;
  auto count = ma.size();
  std::vector<bool> visited(count);
  auto begin = ma.cbegin();
  auto end = ma.cend();
  size_t i = 0;
  std::map<Id, MeasArray> result;
  MeasArray current_id_values;
  current_id_values.resize(ma.size());

  ENSURE(current_id_values.size() != 0);
  ENSURE(current_id_values.size() == ma.size());

  for (auto it = begin; it != end; ++it, ++i) {
    if (visited[i]) {
      continue;
    }
    if (dropped.find(it->id) != dropped.end()) {
      continue;
    }

    visited[i] = true;
    size_t current_id_values_pos = 0;
    current_id_values[current_id_values_pos++] = *it;
    size_t pos = 0;
    for (auto sub_it = begin; sub_it != end; ++sub_it, ++pos) {
      if (visited[pos]) {
        continue;
      }
      if ((sub_it->id == it->id)) {
        current_id_values[current_id_values_pos++] = *sub_it;
        visited[pos] = true;
      }
    }
    dropped.insert(it->id);
    result.insert(std::make_pair(
        it->id, MeasArray{current_id_values.begin(),
                          current_id_values.begin() + current_id_values_pos}));
    current_id_values_pos = 0;
  }
  return result;
}

std::list<HdrAndBuffer> compressValues(std::map<Id, MeasArray> &to_compress,
                                       PageFooter &phdr,
                                       uint32_t max_chunk_size) {
  using namespace dariadb::utils::async;
  std::list<HdrAndBuffer> results;
  utils::async::Locker result_locker;
  std::list<utils::async::TaskResult_Ptr> async_compressions;
  for (auto &kv : to_compress) {
    auto cur_Id = kv.first;
    utils::async::AsyncTask at = [cur_Id, &results, &phdr, max_chunk_size,
                                  &result_locker, &to_compress](
        const utils::async::ThreadInfo &ti) {
      using namespace dariadb::utils::async;
      TKIND_CHECK(dariadb::utils::async::THREAD_KINDS::COMMON, ti.kind);
      auto fit = to_compress.find(cur_Id);
      auto begin = fit->second.cbegin();
      auto end = fit->second.cend();
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

        result_locker.lock();
        phdr.max_chunk_id++;

        ch->header->id = phdr.max_chunk_id;

        phdr.stat.update(ch->header->stat);

        HdrAndBuffer subres;
        subres.hdr = hdr;
        subres.buffer = buffer_ptr;

        results.push_back(subres);
        result_locker.unlock();
      }
      return false;
    };
    auto cur_async =
        ThreadManager::instance()->post(THREAD_KINDS::COMMON, AT(at));
    async_compressions.push_back(cur_async);
  }
  for (auto tr : async_compressions) {
    tr->wait();
  }
  return results;
}

uint64_t writeToFile(FILE *file, FILE *index_file, PageFooter &phdr,
                     IndexFooter &ihdr,
                     std::list<HdrAndBuffer> &compressed_results,
                     uint64_t file_size) {

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
    std::fwrite(chunk_buffer_ptr.get() + skip_count, sizeof(uint8_t),
                chunk_header.size, file);

    offset += sizeof(ChunkHeader) + chunk_header.size;

    auto index_reccord = init_chunk_index_rec(chunk_header, &ihdr);
    ireccords[pos] = index_reccord;
    pos++;
  }
  std::fwrite(ireccords.data(), sizeof(IndexReccord), ireccords.size(),
              index_file);
  page_size = offset;
  return page_size;
}

IndexReccord init_chunk_index_rec(const ChunkHeader &cheader,
                                  IndexFooter *iheader) {
  IndexReccord cur_index;

  cur_index.chunk_id = cheader.id;
  cur_index.offset = cheader.offset_in_page; // header->write_offset;

  iheader->stat.update(cheader.stat);

  iheader->id_bloom = storage::bloom_add(iheader->id_bloom, cheader.meas_id);
  iheader->recs_count++;
  cur_index.meas_id = cheader.meas_id;
  cur_index.stat = cheader.stat;
  ENSURE(cur_index.stat.minTime <= cur_index.stat.maxTime);
  ENSURE(cur_index.stat.minValue <= cur_index.stat.maxValue);
  return cur_index;
}
}
}
}