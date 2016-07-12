#include "page.h"
#include "../timeutil.h"
#include "../utils/metrics.h"
#include "bloom_filter.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <fstream>

using namespace dariadb::storage;

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

Page *Page::create(std::string file_name, uint64_t sz, uint32_t chunk_per_storage,
                   uint32_t chunk_size) {
  TIMECODE_METRICS(ctmd, "create", "Page::create");
  auto res = new Page;
  res->readonly = false;
  auto mmap = utils::fs::MappedFile::touch(file_name, sz);
  res->filename = file_name;
  auto region = mmap->data();
  std::fill(region, region + sz, 0);

  res->page_mmap = mmap;
  res->_index = PageIndex::create(PageIndex::index_name_from_page_name(file_name),
                                  index_file_size(chunk_per_storage), chunk_per_storage,
                                  chunk_size);
  res->region = region;

  res->header = reinterpret_cast<PageHeader *>(region);
  res->chunks = reinterpret_cast<uint8_t *>(region + sizeof(PageHeader));
  res->header->minTime = dariadb::MAX_TIME;
  res->header->maxTime = dariadb::MIN_TIME;
  res->header->chunk_per_storage = chunk_per_storage;
  res->header->chunk_size = chunk_size;
  res->header->is_closed = false;
  res->header->is_open_to_write = true;
  for (uint32_t i = 0; i < res->header->chunk_per_storage; ++i) {
    res->_free_poses.push_back(i);
  }

  res->page_mmap->flush(0, sizeof(PageHeader));
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
  if (res->header->chunk_size == 0) {
    throw MAKE_EXCEPTION("(res->header->chunk_size == 0)");
  }

  for (uint32_t i = 0; i < res->header->chunk_per_storage; ++i) {
    auto irec = &res->_index->index[i];
    if (!irec->is_init) {
      res->_free_poses.push_back(i);
    } else {
      auto kv = std::make_pair(irec->maxTime, i);
      res->_index->_itree.insert(kv);
    }
  }
  /*assert(res->header->is_closed);*/

  return res;
}

PageHeader Page::readHeader(std::string file_name) {
  std::ifstream istream;
  istream.open(file_name, std::fstream::in | std::fstream::binary);
  if (!istream.is_open()) {
    std::stringstream ss;
    ss << "can't open file. filename=" << file_name;
    throw MAKE_EXCEPTION(ss.str());
  }
  PageHeader result;
  memset(&result, 0, sizeof(PageHeader));
  istream.read((char *)&result, sizeof(PageHeader));
  istream.close();
  return result;
}

IndexHeader Page::readIndexHeader(std::string ifile) {
  std::ifstream istream;
  istream.open(ifile, std::fstream::in | std::fstream::binary);
  if (!istream.is_open()) {
    std::stringstream ss;
    ss << "can't open file. filename=" << ifile;
    throw MAKE_EXCEPTION(ss.str());
  }
  IndexHeader result;
  memset(&result, 0, sizeof(IndexHeader));
  istream.read((char *)&result, sizeof(IndexHeader));
  istream.close();
  return result;
}

void Page::fsck() {
  using dariadb::timeutil::to_string;
  logger_info("fsck: page " << this->filename);

  auto step = this->header->chunk_size + sizeof(ChunkHeader);
  auto byte_it = this->chunks;
  auto end = this->chunks + this->header->chunk_per_storage * step;
  size_t pos = 0;
  while (true) {
    if (byte_it == end) {
      break;
    }
    ChunkHeader *info = reinterpret_cast<ChunkHeader *>(byte_it);
    auto ptr_to_begin = byte_it;
    auto ptr_to_buffer = ptr_to_begin + sizeof(ChunkHeader);
    if (info->is_init) {
      Chunk_Ptr ptr = nullptr;
      ptr = Chunk_Ptr{new ZippedChunk(info, ptr_to_buffer)};
      if ((!ptr->header->commit) || !ptr->check_checksum()) {
        logger_fatal("fsck: remove broken chunk #"
                     << ptr->header->id << " id:" << ptr->header->first.id << " time: ["
                     << to_string(ptr->header->minTime) << " : "
                     << to_string(ptr->header->maxTime) << "]");
        ptr->header->is_init = false;
        _index->iheader->is_sorted = false;
        _index->index[pos].is_init = false;
      }
    }
    ++pos;
    byte_it += step;
  }
  header->transaction = 0;
  _index->iheader->transaction = 0;
}

bool Page::add_to_target_chunk(const dariadb::Meas &m) {
  TIMECODE_METRICS(ctmd, "append", "Page::add_to_target_chunk");
  assert(!this->readonly);
  std::lock_guard<std::mutex> lg(_locker);
  if (is_full()) {
    header->is_full = true;
    return false;
  }

  if (_openned_chunk.ch != nullptr && !_openned_chunk.ch->is_full()) {
    if (_openned_chunk.ch->header->last.id != m.id) {
      close_corrent_chunk();
    } else {
      if (_openned_chunk.ch->append(m)) {
        this->header->minTime = std::min(m.time, this->header->minTime);
        this->header->maxTime = std::max(m.time, this->header->maxTime);
        flush_current_chunk();
        _index->update_index_info(_openned_chunk.index, _openned_chunk.ch, m,
                                  _openned_chunk.pos);
        return true;
	  }
	  else {
		  close_corrent_chunk();
	  }
    }
  }
  // search no full chunk.
  auto step = this->header->chunk_size + sizeof(ChunkHeader);
  auto byte_it = this->chunks + step * this->header->addeded_chunks;
  auto end = this->chunks + this->header->chunk_per_storage * step;
  while (true) {
    if (byte_it == end) {
      header->is_full = true;
      break;
    }
    ChunkHeader *info = reinterpret_cast<ChunkHeader *>(byte_it);
    if (!info->is_init) {
      auto ptr_to_begin = byte_it;
      auto ptr_to_buffer = ptr_to_begin + sizeof(ChunkHeader);
      Chunk_Ptr ptr = nullptr;
      ptr = Chunk_Ptr{new ZippedChunk(info, ptr_to_buffer, header->chunk_size, m)};

      this->header->max_chunk_id++;
      ptr->header->id = this->header->max_chunk_id;
      if (header->_under_transaction) {
        ptr->header->transaction = header->transaction;
        ptr->header->commit = false;
      } else {
        ptr->header->commit = true;
      }
      _openned_chunk.ch = ptr;
      flush_current_chunk();
      init_chunk_index_rec(ptr);
      return true;
    }
    byte_it += step;
  }
  header->is_full = true;
  return false;
}

void Page::flush_current_chunk() {
  /*if(addeded_meases%PAGE_FLUSH_PERIOD==0){
      auto offset=(uint8_t*)_openned_chunk.ch->header-this->region;
      page_mmap->flush(offset,_openned_chunk.ch->header->size);
      page_mmap->flush(0,sizeof(PageHeader));
  }*/
}

void Page::close_corrent_chunk() {
  if (_openned_chunk.ch != nullptr) {
    flush_current_chunk();
    _openned_chunk.ch->close();
#ifdef ENABLE_METRICS
    // calc perscent of free space in closed chunks
	auto used_space = _openned_chunk.ch->header->size - _openned_chunk.ch->header->bw_pos;
	auto size = _openned_chunk.ch->header->size;
    auto percent = used_space * float(100.0) / size;
    ADD_METRICS("write", "chunk_free",
                dariadb::utils::metrics::Metric_Ptr{
                    new dariadb::utils::metrics::FloatMetric(percent)});
#endif
    _openned_chunk.ch = nullptr;
  }
}

void Page::init_chunk_index_rec(Chunk_Ptr ch) {
  TIMECODE_METRICS(ctmd, "write", "Page::init_chunk_index_rec");
  assert(header->chunk_size == ch->header->size);

  uint32_t pos_index = 0;

  pos_index = _free_poses.front();
  _free_poses.pop_front();

  auto cur_index = &_index->index[pos_index];
  cur_index->chunk_id = ch->header->id;
  cur_index->is_init = true;
  cur_index->offset = header->pos;

  header->pos += header->chunk_size + sizeof(ChunkHeader);
  header->addeded_chunks++;
  header->transaction = std::max(header->transaction, ch->header->transaction);
  header->minTime = std::min(header->minTime, ch->header->minTime);
  header->maxTime = std::max(header->minTime, ch->header->maxTime);

  _index->iheader->minTime = std::min(_index->iheader->minTime, ch->header->minTime);
  _index->iheader->maxTime = std::max(_index->iheader->maxTime, ch->header->maxTime);
  _index->iheader->id_bloom =
      storage::bloom_add(_index->iheader->id_bloom, ch->header->first.id);
  _index->iheader->flag_bloom =
      storage::bloom_add(_index->iheader->flag_bloom, ch->header->first.flag);
  _index->iheader->count++;

  cur_index->minTime = ch->header->minTime;
  cur_index->maxTime = cur_index->maxTime;
  cur_index->id_bloom = ch->header->id_bloom;
  cur_index->flag_bloom = ch->header->flag_bloom;
  cur_index->transaction = ch->header->transaction;
  cur_index->commit = ch->header->commit;

  auto kv = std::make_pair(cur_index->maxTime, pos_index);
  _index->_itree.insert(kv);

  _openned_chunk.index = cur_index;
  _openned_chunk.pos = pos_index;
}

bool Page::is_full() const {
  return this->_free_poses.empty() &&
         (_openned_chunk.ch == nullptr || _openned_chunk.ch->is_full());
}

void Page::dec_reader() {
  std::lock_guard<std::mutex> lg(_locker);
  header->count_readers--;
}

bool dariadb::storage::Page::minMaxTime(dariadb::Id id, dariadb::Time *minTime,
                                        dariadb::Time *maxTime) {
  QueryInterval qi{dariadb::IdArray{id}, 0, this->header->minTime, this->header->maxTime};
  auto all_chunks = this->chunksByIterval(qi);

  bool result = false;
  if (!all_chunks.empty()) {
	  result = true;
  }
  *minTime = dariadb::MAX_TIME;
  *maxTime = dariadb::MIN_TIME;
  for (auto&link : all_chunks) {
	  auto _index_it = this->_index->index[link.pos];
	  *minTime = std::min(*minTime, _index_it.minTime);
	  *maxTime = std::max(*maxTime, _index_it.maxTime);
  }
  return result;
}

ChunkLinkList dariadb::storage::Page::chunksByIterval(const QueryInterval &query) {
  return _index->get_chunks_links(query.ids, query.from, query.to, query.flag);
}

dariadb::Meas::Id2Meas Page::valuesBeforeTimePoint(const QueryTimePoint &q) {
  TIMECODE_METRICS(ctmd, "readInTimePoint", "Page::valuesBeforeTimePoint");
  dariadb::Meas::Id2Meas result;
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
    auto _index_it = this->_index->index[it->pos];
    auto ptr_to_begin = this->chunks + _index_it.offset;
    auto ptr_to_chunk_info_raw = reinterpret_cast<ChunkHeader *>(ptr_to_begin);
    auto ptr_to_buffer_raw = ptr_to_begin + sizeof(ChunkHeader);

    Chunk_Ptr ptr = nullptr;
    if (ptr_to_chunk_info_raw->is_zipped) {
      ptr = Chunk_Ptr{new ZippedChunk(ptr_to_chunk_info_raw, ptr_to_buffer_raw)};
    } else {
      assert(false);
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
                     ReaderClb *clb) {
  TIMECODE_METRICS(ctmd, "readLinks", "Page::readLinks");
  std::lock_guard<std::mutex> lg(_locker);
  auto _ch_links_iterator = links.cbegin();
  if (_ch_links_iterator == links.cend()) {
    return;
  }

  for (; _ch_links_iterator != links.cend(); ++_ch_links_iterator) {
    auto _index_it = this->_index->index[_ch_links_iterator->pos];
    Chunk_Ptr search_res;
    // if (!ChunkCache::instance()->find(_index_it.chunk_id, search_res))
    {
      auto ptr_to_begin = this->chunks + _index_it.offset;
      auto ptr_to_chunk_info_raw = reinterpret_cast<ChunkHeader *>(ptr_to_begin);
      auto ptr_to_buffer_raw = ptr_to_begin + sizeof(ChunkHeader);
      if (!ptr_to_chunk_info_raw->is_init) {
        logger_info("Try to read not_init chunk (" << ptr_to_chunk_info_raw->id
                                                   << "). maybe broken");
        continue;
      }

      Chunk_Ptr ptr = nullptr;
      if (ptr_to_chunk_info_raw->is_zipped) {
        ptr = Chunk_Ptr{new ZippedChunk(ptr_to_chunk_info_raw, ptr_to_buffer_raw)};
        // ptr->should_free = true;
      } else {
        assert(false);
      }
      Chunk_Ptr c{ptr};
      /* if (c->header->is_readonly && !c->check_checksum()) {
         logger("page: " << this->filename << ": "
                         << "wrong chunk checksum. chunkId=" << c->header->id);
         continue;
       }*/
      search_res = c;
    }
    auto rdr = search_res->get_reader();
    while (!rdr->is_end()) {
      auto subres = rdr->readNext();
      if (search_res->header->is_sorted && subres.time > query.to) {
        break;
      }
      if (subres.inQuery(query.ids, query.flag, query.source, query.from, query.to)) {
        clb->call(subres);
      }
    }
  }
}

dariadb::append_result dariadb::storage::Page::append(const Meas &value) {
  TIMECODE_METRICS(ctmd, "append", "Page::append");
  if (add_to_target_chunk(value)) {
    return dariadb::append_result(1, 0);
  } else {
    return dariadb::append_result(0, 1);
  }
}

void dariadb::storage::Page::flush() {}

void dariadb::storage::Page::begin_transaction(uint64_t) {
  if (_openned_chunk.ch != nullptr) {
    close_corrent_chunk();
    this->page_mmap->flush();
    this->_index->index_mmap->flush();
  }
}

void dariadb::storage::Page::commit_transaction(uint64_t num) {
  // logger_info("page: commit transaction " << num << " " << this->filename);

  close_corrent_chunk();
  auto step = this->header->chunk_size + sizeof(ChunkHeader);
  auto byte_it = this->chunks;
  auto end = this->chunks + this->header->addeded_chunks * step;
  size_t pos = 0;
  // size_t chunks_count = 0;
  while (true) {
    if (byte_it == end) {
      break;
    }
    ChunkHeader *info = reinterpret_cast<ChunkHeader *>(byte_it);
    auto ptr_to_begin = byte_it;
    auto ptr_to_buffer = ptr_to_begin + sizeof(ChunkHeader);
    if (info->is_init) {
      Chunk_Ptr ptr = nullptr;
      ptr = Chunk_Ptr{new ZippedChunk(info, ptr_to_buffer)};
      if (ptr->header->transaction == num) {
        ptr->header->commit = true;
        _index->index[pos].commit = true;
        this->page_mmap->flush();
        this->_index->index_mmap->flush();
        //++chunks_count;
      }
    }
    ++pos;
    byte_it += step;
  }
  header->_under_transaction = false;
  this->page_mmap->flush();
  this->_index->index_mmap->flush();

  // logger_info("commit in " << chunks_count << " chunks.");
}

void dariadb::storage::Page::rollback_transaction(uint64_t num) {
  logger_info("page: rollback transaction " << num << " " << this->filename);
  auto step = this->header->chunk_size + sizeof(ChunkHeader);
  auto byte_it = this->chunks;
  auto end = this->chunks + this->header->addeded_chunks * step;
  size_t pos = 0;
  size_t chunks_count = 0;
  while (true) {
    if (byte_it == end) {
      break;
    }
    ChunkHeader *info = reinterpret_cast<ChunkHeader *>(byte_it);
    auto ptr_to_begin = byte_it;
    auto ptr_to_buffer = ptr_to_begin + sizeof(ChunkHeader);
    if (info->is_init) {
      Chunk_Ptr ptr = nullptr;
      ptr = Chunk_Ptr{new ZippedChunk(info, ptr_to_buffer)};
      if (ptr->header->transaction == num) {
        ptr->header->is_init = false;
        ptr->header->commit = true;
        _index->index[pos].commit = true;
        _index->iheader->is_sorted = false;
        _index->index[pos].is_init = false;
        this->page_mmap->flush();
        this->_index->index_mmap->flush();
        ++chunks_count;
      }
    }
    ++pos;
    byte_it += step;
  }
  header->_under_transaction = false;
  this->page_mmap->flush();
  this->_index->index_mmap->flush();

  logger_info("rollback in " << chunks_count << " chunks.");
}