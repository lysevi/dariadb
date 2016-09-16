#include "page.h"
#include "../timeutil.h"
#include "../utils/exception.h"
#include "../utils/metrics.h"
#include "bloom_filter.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <fstream>

using namespace dariadb::storage;
using namespace dariadb;

namespace PageInner {
using HdrAndBuffer = std::tuple<ChunkHeader, std::shared_ptr<uint8_t>>;

std::list<MeasList> splitById(const MeasArray &ma) {
  dariadb::IdSet dropped;
  auto count = ma.size();
  std::vector<bool> visited(count);
  auto begin = ma.cbegin();
  auto end = ma.cend();
  size_t i = 0;
  std::list<MeasList> result;
  for (auto it = begin; it != end; ++it, ++i) {
    if (visited[i]) {
      continue;
    }
    if (dropped.find(it->id) != dropped.end()) {
      continue;
    }
    MeasList current_id_values;
    visited[i] = true;
    current_id_values.push_back(*it);
    size_t pos = 0;
    for (auto sub_it = begin; sub_it != end; ++sub_it, ++pos) {
      if (visited[pos]) {
        continue;
      }
      if ((sub_it->id == it->id)) {
        current_id_values.push_back(*sub_it);
        visited[pos] = true;
      }
    }
    dropped.insert(it->id);
    result.push_back(std::move(current_id_values));
  }
  return result;
}

std::list<HdrAndBuffer> compressValues(std::list<MeasList> &to_compress,
                                       PageHeader &phdr, uint32_t max_chunk_size) {
  std::list<HdrAndBuffer> results;

  for (auto &lst : to_compress) {
    auto it = lst.cbegin();
    while (it != lst.cend()) {
      ChunkHeader hdr;
      memset(&hdr, 0, sizeof(ChunkHeader));
      auto buff_size = max_chunk_size;
      std::shared_ptr<uint8_t> buffer_ptr{new uint8_t[buff_size]};
      memset(buffer_ptr.get(), 0, buff_size);
      ZippedChunk ch(&hdr, buffer_ptr.get(), buff_size, *it);
      ++it;
      while (it != lst.cend()) {
        if (!ch.append(*it)) {
          break;
        }
        ++it;
      }
      ch.close();
      phdr.max_chunk_id++;
      phdr.minTime = std::min(phdr.minTime, ch.header->minTime);
      phdr.maxTime = std::max(phdr.maxTime, ch.header->maxTime);
      ch.header->id = phdr.max_chunk_id;

      HdrAndBuffer subres = std::make_tuple(hdr, buffer_ptr);
      results.push_back(subres);
    }
  }
  return results;
}

/// result - page file size in bytes
uint64_t writeToFile(const std::string &file_name, PageHeader &phdr,
                     std::list<HdrAndBuffer> &compressed_results) {
  auto file = std::fopen(file_name.c_str(), "ab");
  if (file == nullptr) {
    throw MAKE_EXCEPTION("aofile: append error.");
  }

  std::fwrite(&(phdr), sizeof(PageHeader), 1, file);
  uint64_t offset = 0;

  for (auto hb : compressed_results) {
    ChunkHeader chunk_header = std::get<0>(hb);
    std::shared_ptr<uint8_t> chunk_buffer_ptr = std::get<1>(hb);

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
  auto page_size = offset + sizeof(PageHeader);
  phdr.filesize = page_size;
  std::fclose(file);
  return page_size;
}
}

Page::~Page() {
  if (!this->readonly) {
    if (this->_openned_chunk.ch != nullptr) {
      this->_openned_chunk.ch->close();
    }

    header->is_closed = true;
    header->is_open_to_write = false;
  }
  region = nullptr;
  header = nullptr;
  _index = nullptr;
  chunks = nullptr;
  page_mmap->close();
}

uint64_t index_file_size(uint32_t chunk_per_storage) {
  return chunk_per_storage * sizeof(IndexReccord) + sizeof(IndexHeader);
}

Page *Page::create(const std::string &file_name, uint64_t chunk_id,
                   uint32_t max_chunk_size, const MeasArray &ma) {
  TIMECODE_METRICS(ctmd, "create", "Page::create(array)");

  std::list<MeasList> to_compress = PageInner::splitById(ma);

  PageHeader phdr;
  memset(&phdr, 0, sizeof(PageHeader));
  phdr.maxTime = dariadb::MIN_TIME;
  phdr.minTime = dariadb::MAX_TIME;
  phdr.max_chunk_id = chunk_id;

  std::list<PageInner::HdrAndBuffer> compressed_results =
      PageInner::compressValues(to_compress, phdr, max_chunk_size);

  auto page_size = PageInner::writeToFile(file_name, phdr, compressed_results);
  phdr.filesize = page_size;

  auto res = new Page;
  res->readonly = false;
  auto mmap = utils::fs::MappedFile::open(file_name, false);
  res->filename = file_name;
  auto region = mmap->data();

  res->page_mmap = mmap;
  res->_index = PageIndex::create(PageIndex::index_name_from_page_name(file_name),
                                  index_file_size(phdr.addeded_chunks));
  res->region = region;

  res->header = reinterpret_cast<PageHeader *>(region);
  *(res->header) = phdr;
  res->chunks = reinterpret_cast<uint8_t *>(region + sizeof(PageHeader));
  res->readonly = false;
  res->header->is_closed = true;
  res->header->is_open_to_write = false;
  res->page_mmap->flush(0, sizeof(PageHeader));
  res->update_index_recs();
  res->flush();
  assert(res->header->addeded_chunks == compressed_results.size());
  return res;
}

Page *Page::open(std::string file_name, bool read_only) {
  TIMECODE_METRICS(ctmd, "open", "Page::open");
  auto res = new Page;
  res->readonly = read_only;
  auto mmap = utils::fs::MappedFile::open(file_name, read_only);
  res->filename = file_name;
  auto region = mmap->data();

  res->page_mmap = mmap;
  res->_index =
      PageIndex::open(PageIndex::index_name_from_page_name(file_name), read_only);

  res->region = region;
  res->header = reinterpret_cast<PageHeader *>(region);
  if (!res->readonly) {
    res->header->is_open_to_write = true;
    res->header->is_closed = false;
  }
  res->chunks = reinterpret_cast<uint8_t *>(region + sizeof(PageHeader));
  res->page_mmap->flush(0, sizeof(PageHeader));
  res->check_page_struct();
  return res;
}

void Page::check_page_struct() {
#ifdef DEBUG
  for (uint32_t i = 0; i < header->addeded_chunks; ++i) {
    auto irec = &_index->index[i];
    if (irec->is_init) {

      ChunkHeader *info = reinterpret_cast<ChunkHeader *>(chunks + irec->offset);
      if (info->id != irec->chunk_id) {
        throw MAKE_EXCEPTION("(info->id != irec->chunk_id)");
      }
      if (info->pos_in_page != i) {
        throw MAKE_EXCEPTION("(info->pos_in_page != i)");
      }
    }
  }
#endif
}
PageHeader Page::readHeader(std::string file_name) {
  std::ifstream istream;
  istream.open(file_name, std::fstream::in | std::fstream::binary);
  if (!istream.is_open()) {
    THROW_EXCEPTION_SS("can't open file. filename=" << file_name);
  }
  PageHeader result;
  memset(&result, 0, sizeof(PageHeader));
  istream.read((char *)&result, sizeof(PageHeader));
  istream.close();
  return result;
}

IndexHeader Page::readIndexHeader(std::string ifile) {
  return PageIndex::readIndexHeader(ifile);
}

void Page::fsck() {
  using dariadb::timeutil::to_string;
  logger_info("engine: fsck page ", this->filename);

  auto byte_it = this->chunks;
  auto end = this->region + this->header->filesize;
  while (true) {
    if (byte_it >= end) {
      break;
    }
    ChunkHeader *info = reinterpret_cast<ChunkHeader *>(byte_it);
    if (info->is_init) {
      auto ptr_to_begin = byte_it;
      auto ptr_to_buffer = ptr_to_begin + sizeof(ChunkHeader);
      Chunk_Ptr ptr = nullptr;
      ptr = Chunk_Ptr{new ZippedChunk(info, ptr_to_buffer)};

      if (!ptr->check_checksum()) {
        logger_fatal("engine: fsck remove broken chunk #", ptr->header->id, " id:",
                     ptr->header->first.id, " time: [", to_string(ptr->header->minTime),
                     " : ", to_string(ptr->header->maxTime), "]");
        mark_as_non_init(ptr);
      }
    }
    byte_it += sizeof(ChunkHeader) + info->size;
  }
}

void Page::update_index_recs() {
  auto byte_it = this->chunks;
  auto end = this->region + this->header->filesize;
  while (true) {
    if (byte_it == end) {
      header->is_full = true;
      break;
    }
    ChunkHeader *info = reinterpret_cast<ChunkHeader *>(byte_it);
    if (info->is_init) {
      auto ptr_to_begin = byte_it;
      auto ptr_to_buffer = ptr_to_begin + sizeof(ChunkHeader);
      Chunk_Ptr ptr = nullptr;
      ptr = Chunk_Ptr{new ZippedChunk(info, ptr_to_buffer)};

      this->header->max_chunk_id++;
      ptr->header->id = this->header->max_chunk_id;

      init_chunk_index_rec(ptr, ptr->header->pos_in_page);
      ptr->close();
    }
    byte_it += sizeof(ChunkHeader) + info->size;
  }
  header->is_full = true;
  _index->iheader->is_full = true;
}

void Page::init_chunk_index_rec(Chunk_Ptr ch, uint32_t pos_index) {
  TIMECODE_METRICS(ctmd, "write", "Page::init_chunk_index_rec");

  auto cur_index = &_index->index[pos_index];
  assert(cur_index->chunk_id == 0);
  assert(cur_index->is_init == false);
  ch->header->pos_in_page = pos_index;
  cur_index->chunk_id = ch->header->id;
  cur_index->is_init = true;
  cur_index->offset = ch->header->offset_in_page; // header->write_offset;

  header->minTime = std::min(header->minTime, ch->header->minTime);
  header->maxTime = std::max(header->maxTime, ch->header->maxTime);

  _index->iheader->minTime = std::min(_index->iheader->minTime, ch->header->minTime);
  _index->iheader->maxTime = std::max(_index->iheader->maxTime, ch->header->maxTime);
  _index->iheader->id_bloom =
      storage::bloom_add(_index->iheader->id_bloom, ch->header->first.id);
  _index->iheader->flag_bloom =
      storage::bloom_add(_index->iheader->flag_bloom, ch->header->first.flag);
  _index->iheader->count++;

  cur_index->minTime = ch->header->minTime;
  cur_index->maxTime = ch->header->maxTime;
  cur_index->id_bloom = ch->header->id_bloom;
  cur_index->flag_bloom = ch->header->flag_bloom;

  _openned_chunk.index = cur_index;
  _openned_chunk.pos = pos_index;
}

bool Page::is_full() const {
  return this->header->is_full &&
         (_openned_chunk.ch == nullptr || _openned_chunk.ch->is_full());
}

bool Page::minMaxTime(dariadb::Id id, dariadb::Time *minTime, dariadb::Time *maxTime) {
  QueryInterval qi{dariadb::IdArray{id}, 0, this->header->minTime, this->header->maxTime};
  auto all_chunks = this->chunksByIterval(qi);

  bool result = false;
  if (!all_chunks.empty()) {
    result = true;
  }
  *minTime = dariadb::MAX_TIME;
  *maxTime = dariadb::MIN_TIME;
  for (auto &link : all_chunks) {
    auto _index_it = this->_index->index[link.index_rec_number];
    *minTime = std::min(*minTime, _index_it.minTime);
    *maxTime = std::max(*maxTime, _index_it.maxTime);
  }
  return result;
}

ChunkLinkList Page::chunksByIterval(const QueryInterval &query) {
  return _index->get_chunks_links(query.ids, query.from, query.to, query.flag);
}

dariadb::Id2Meas Page::valuesBeforeTimePoint(const QueryTimePoint &q) {
  TIMECODE_METRICS(ctmd, "readTimePoint", "Page::valuesBeforeTimePoint");
  dariadb::Id2Meas result;
  auto raw_links =
      _index->get_chunks_links(q.ids, _index->iheader->minTime, q.time_point, q.flag);
  if (raw_links.empty()) {
    return result;
  }

  dariadb::IdSet to_read{q.ids.begin(), q.ids.end()};
  for (auto it = raw_links.rbegin(); it != raw_links.rend(); ++it) {
    if (to_read.empty()) {
      break;
    }
    auto _index_it = this->_index->index[it->index_rec_number];
    auto ptr_to_begin = this->chunks + _index_it.offset;
    auto ptr_to_chunk_info_raw = reinterpret_cast<ChunkHeader *>(ptr_to_begin);
    auto ptr_to_buffer_raw = ptr_to_begin + sizeof(ChunkHeader);

    Chunk_Ptr ptr = nullptr;
    if (ptr_to_chunk_info_raw->kind == ChunkKind::Compressed) {
      ptr = Chunk_Ptr{new ZippedChunk(ptr_to_chunk_info_raw, ptr_to_buffer_raw)};
    } else {
      THROW_EXCEPTION_SS("Unknow ChunkKind: " << ptr_to_chunk_info_raw->kind);
    }

    Chunk_Ptr c{ptr};
    auto reader = c->get_reader();
    while (!reader->is_end()) {
      auto m = reader->readNext();
      if (m.time <= q.time_point && m.inQuery(q.ids, q.flag, q.source)) {
        auto f_res = result.find(m.id);
        if (f_res == result.end()) {
          to_read.erase(m.id);
          result[m.id] = m;
        } else {
          if (m.time > f_res->first) {
            result[m.id] = m;
          }
        }
      }
    }
  }
  return result;
}

void Page::readLinks(const QueryInterval &query, const ChunkLinkList &links,
                     IReaderClb *clb) {
  TIMECODE_METRICS(ctmd, "readLinks", "Page::readLinks");
  auto _ch_links_iterator = links.cbegin();
  if (_ch_links_iterator == links.cend()) {
    return;
  }

  for (; _ch_links_iterator != links.cend(); ++_ch_links_iterator) {
    auto _index_it = this->_index->index[_ch_links_iterator->index_rec_number];
    Chunk_Ptr search_res;

    auto ptr_to_begin = this->chunks + _index_it.offset;
    auto ptr_to_chunk_info_raw = reinterpret_cast<ChunkHeader *>(ptr_to_begin);
    auto ptr_to_buffer_raw = ptr_to_begin + sizeof(ChunkHeader);
    if (!ptr_to_chunk_info_raw->is_init) {
      logger_info("engine: Try to read not_init chunk (", ptr_to_chunk_info_raw->id,
                  "). maybe broken");
      continue;
    }

    Chunk_Ptr ptr = nullptr;
    if (ptr_to_chunk_info_raw->kind == ChunkKind::Compressed) {
      ptr = Chunk_Ptr{new ZippedChunk(ptr_to_chunk_info_raw, ptr_to_buffer_raw)};
    } else {
      THROW_EXCEPTION_SS("Unknow ChunkKind: " << ptr_to_chunk_info_raw->kind);
    }
    Chunk_Ptr c{ptr};
    search_res = c;
    auto rdr = search_res->get_reader();
    while (!rdr->is_end()) {
      auto subres = rdr->readNext();
      if (subres.time > query.to) {
        break;
      }
      if (subres.inQuery(query.ids, query.flag, query.source, query.from, query.to)) {
        clb->call(subres);
      }
    }
  }
}

void Page::flush() {
  this->page_mmap->flush();
  this->_index->index_mmap->flush();
}

void Page::mark_as_non_init(Chunk_Ptr &ptr) {
  ptr->header->is_init = false;
  auto pos = ptr->header->pos_in_page;
  _index->iheader->is_sorted = false;
  _index->index[pos].is_init = false;
  this->header->removed_chunks++;
}
