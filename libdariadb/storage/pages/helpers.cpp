#ifdef MSVC
#define _CRT_SECURE_NO_WARNINGS // for fopen
#endif
#include <libdariadb/meas.h>
#include <libdariadb/storage/pages/helpers.h>
#include <libdariadb/storage/pages/page.h>
#include <libdariadb/utils/thread_manager.h>
#include <algorithm>
#include <cassert>
#include <cstring>
#include <fstream>

namespace dariadb {
namespace storage {
namespace PageInner {

dariadb::storage::PageHeader emptyPageHeader(uint64_t chunk_id) {
  dariadb::storage::PageHeader phdr;
  memset(&phdr, 0, sizeof(PageHeader));
  phdr.maxTime = dariadb::MIN_TIME;
  phdr.minTime = dariadb::MAX_TIME;
  phdr.max_chunk_id = chunk_id;

  return phdr;
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

  assert(current_id_values.size() != 0);
  assert(current_id_values.size() == ma.size());

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
                                       PageHeader &phdr, uint32_t max_chunk_size) {
  using namespace dariadb::utils::async;
  std::list<HdrAndBuffer> results;
  utils::Locker result_locker;
  std::list<utils::async::TaskResult_Ptr> async_compressions;
  for (auto &kv : to_compress) {
    auto cur_Id = kv.first;
    utils::async::AsyncTask at = [cur_Id, &results, &phdr, max_chunk_size, &result_locker,
                                  &to_compress](const utils::async::ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::COMMON, ti.kind);
      auto fit = to_compress.find(cur_Id);
      auto begin = fit->second.cbegin();
      auto end = fit->second.cend();
      auto it = begin;
      while (it != end) {
        ChunkHeader hdr;
        memset(&hdr, 0, sizeof(ChunkHeader));
        // TODO use memory_pool
        std::shared_ptr<uint8_t> buffer_ptr{new uint8_t[max_chunk_size]};
        memset(buffer_ptr.get(), 0, max_chunk_size);
        ZippedChunk ch(&hdr, buffer_ptr.get(), max_chunk_size, *it);
        ++it;
        while (it != end) {
          if (!ch.append(*it)) {
            break;
          }
          ++it;
        }
        ch.close();

        result_locker.lock();
        phdr.max_chunk_id++;
        phdr.minTime = std::min(phdr.minTime, ch.header->minTime);
        phdr.maxTime = std::max(phdr.maxTime, ch.header->maxTime);
        ch.header->id = phdr.max_chunk_id;

        HdrAndBuffer subres{hdr, buffer_ptr};

        results.push_back(subres);
        result_locker.unlock();
      }
    };
    auto cur_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::COMMON, AT(at));
    async_compressions.push_back(cur_async);
  }
  for (auto tr : async_compressions) {
    tr->wait();
  }
  return results;
}

uint64_t writeToFile(const std::string &file_name, PageHeader &phdr,
                     std::list<HdrAndBuffer> &compressed_results, uint64_t file_size,
                     bool add_header_size_to_result) {

  using namespace dariadb::utils::async;
  uint64_t page_size = 0;
  auto file = std::fopen(file_name.c_str(), "ab");
  if (file == nullptr) {
    throw MAKE_EXCEPTION("aofile: append error.");
  }

  if (file_size == uint64_t(0)) {
    std::fwrite(&(phdr), sizeof(PageHeader), 1, file);
  }
  uint64_t offset = file_size;

  for (auto hb : compressed_results) {
    ChunkHeader chunk_header = hb.hdr;
    std::shared_ptr<uint8_t> chunk_buffer_ptr = hb.buffer;

    chunk_header.pos_in_page = phdr.addeded_chunks;
    phdr.addeded_chunks++;
    auto cur_chunk_buf_size = chunk_header.size - chunk_header.bw_pos + 1;
    auto skip_count = chunk_header.size - cur_chunk_buf_size;

    chunk_header.size = cur_chunk_buf_size;
    chunk_header.offset_in_page = offset;

    ZippedChunk ch(&chunk_header, chunk_buffer_ptr.get());
    ch.close();
    std::fwrite(&(chunk_header), sizeof(ChunkHeader), 1, file);
    std::fwrite(chunk_buffer_ptr.get() + skip_count, sizeof(uint8_t), cur_chunk_buf_size,
                file);

    offset += sizeof(ChunkHeader) + cur_chunk_buf_size;
  }
  page_size = offset;
  if (add_header_size_to_result) {
    page_size += sizeof(PageHeader);
    phdr.filesize = page_size;
  }
  std::fclose(file);
  return page_size;
}
}
}
}