#include "page.h"
#include "bloom_filter.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <fstream>

using namespace dariadb::storage;

class PageCursor : public dariadb::storage::Cursor {
public:
  PageCursor(Page *page, const ChunkLinkList &chlinks) : link(page), _ch_links(chlinks) {
    reset_pos();
  }

  ~PageCursor() {
    if (link != nullptr) {
      link->dec_reader();
      link = nullptr;
    }
  }

  bool is_end() const override { return _is_end; }

  void readNext(Cursor::Callback *cbk) override {
    std::lock_guard<std::mutex> lg(_locker);
    if (_ch_links_iterator == _ch_links.cend()) {
      _is_end = true;
      return;
    }
    auto _index_it = this->link->_index->index[_ch_links_iterator->pos];
    ++_ch_links_iterator;
    for (; !_is_end;) {
      if (_is_end) {
        Chunk_Ptr empty;
        cbk->call(empty);
        _is_end = true;
        break;
      }

      {
        auto ptr_to_begin = link->chunks + _index_it.offset;
        auto ptr_to_chunk_info_raw = reinterpret_cast<ChunkIndexInfo *>(ptr_to_begin);
        auto ptr_to_buffer_raw = ptr_to_begin + sizeof(ChunkIndexInfo);

        auto info = new ChunkIndexInfo;
        memcpy(info, ptr_to_chunk_info_raw, sizeof(ChunkIndexInfo));
        auto buf = new uint8_t[info->size];
        memcpy(buf, ptr_to_buffer_raw, info->size);
        Chunk_Ptr ptr = nullptr;
        if (info->is_zipped) {
          ptr = Chunk_Ptr{new ZippedChunk(info, buf)};
          ptr->should_free = true;
        } else {
          // TODO implement not zipped page.
          assert(false);
        }
        Chunk_Ptr c{ptr};
        // TODO replace by some check;
        // assert(c->info->last.time != 0);
        cbk->call(c);
        break;
      }
    }
    if (_ch_links_iterator == _ch_links.cend()) {
      _is_end = true;
      return;
    }
  }

  void reset_pos() override { // start read from begining;
    _is_end = false;
    _ch_links_iterator = _ch_links.begin();
  }

protected:
  Page *link;
  bool _is_end;
  std::mutex _locker;
  ChunkLinkList _ch_links;
  ChunkLinkList::const_iterator _ch_links_iterator;
};

Page::~Page() {

  region = nullptr;
  header = nullptr;
  _index = nullptr;
  chunks = nullptr;
  page_mmap->close();
}

uint64_t index_file_size(uint32_t chunk_per_storage) {
  return chunk_per_storage * sizeof(Page_ChunkIndex) + sizeof(IndexHeader);
}

Page *Page::create(std::string file_name, uint64_t sz, uint32_t chunk_per_storage,
                   uint32_t chunk_size) {
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

  res->header->chunk_per_storage = chunk_per_storage;
  res->header->chunk_size = chunk_size;

  for (uint32_t i = 0; i < res->header->chunk_per_storage; ++i) {
    res->_free_poses.push_back(i);
  }
  return res;
}

PageIndex_ptr PageIndex::open(const std::string &filename, bool read_only) {
  PageIndex_ptr res = std::make_shared<PageIndex>();
  res->readonly = read_only;
  auto immap = utils::fs::MappedFile::open(filename);
  auto iregion = immap->data();
  res->index_mmap = immap;
  res->iregion = iregion;
  res->iheader = reinterpret_cast<IndexHeader *>(iregion);
  res->index = reinterpret_cast<Page_ChunkIndex *>(iregion + sizeof(IndexHeader));
  return res;
}


Page *Page::open(std::string file_name, bool read_only) {
  auto res = new Page;
  res->readonly = read_only;
  auto mmap = utils::fs::MappedFile::open(file_name);
  res->filename = file_name;
  auto region = mmap->data();

  res->page_mmap = mmap;
  res->_index =
      PageIndex::open(PageIndex::index_name_from_page_name(file_name), read_only);

  res->region = region;
  res->header = reinterpret_cast<PageHeader *>(region);
  res->chunks = reinterpret_cast<uint8_t *>(region + sizeof(PageHeader));

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

bool Page::add_to_target_chunk(const dariadb::Meas &m) {
  assert(!this->readonly);
  boost::upgrade_lock<boost::shared_mutex> lg(_locker);
  if (is_full()) {
    header->is_full = true;
    return false;
  }

  if (_openned_chunk.ch != nullptr && !_openned_chunk.ch->is_full()) {
    if (_openned_chunk.ch->append(m)) {
      _index->update_index_info(_openned_chunk.index, _openned_chunk.ch, m,
                                _openned_chunk.pos);
      return true;
    }
  }
  // search no full chunk.
  auto step = this->header->chunk_size + sizeof(ChunkIndexInfo);
  auto byte_it = this->chunks + step * this->header->addeded_chunks;
  auto end = this->chunks + this->header->chunk_per_storage * step;
  while (true) {
    if (byte_it == end) {
      header->is_full = true;
      break;
    }
    ChunkIndexInfo *info = reinterpret_cast<ChunkIndexInfo *>(byte_it);
    if (!info->is_init) {
      auto ptr_to_begin = byte_it;
      auto ptr_to_buffer = ptr_to_begin + sizeof(ChunkIndexInfo);
      Chunk_Ptr ptr = nullptr;
      ptr = Chunk_Ptr{new ZippedChunk(info, ptr_to_buffer, header->chunk_size, m)};

      this->header->max_chunk_id++;
      ptr->info->id = this->header->max_chunk_id;
      _openned_chunk.ch = ptr;

      init_chunk_index_rec(ptr);
      return true;
    }
    byte_it += step;
  }
  header->is_full = true;
  return false;
}

void Page::init_chunk_index_rec(Chunk_Ptr ch) {
  assert(header->chunk_size == ch->info->size);

  uint32_t pos_index = 0;

  pos_index = _free_poses.front();
  _free_poses.pop_front();

  auto cur_index = &_index->index[pos_index];
  cur_index->chunk_id = ch->info->id;
  // cur_index->meas_id = ch->info->first.id;
  // cur_index->last = ch->info->last;

  cur_index->flag_bloom = ch->info->flag_bloom;
  //  cur_index->is_readonly = ch->info->is_readonly;
  cur_index->is_init = true;

  cur_index->offset = header->pos;

  header->pos += header->chunk_size + sizeof(ChunkIndexInfo);
  header->addeded_chunks++;

  _index->iheader->minTime = std::min(_index->iheader->minTime, ch->info->minTime);
  _index->iheader->maxTime = std::max(_index->iheader->maxTime, ch->info->maxTime);
  _index->iheader->id_bloom =
      storage::bloom_add(_index->iheader->id_bloom, ch->info->first.id);
  _index->iheader->count++;

  cur_index->minTime = ch->info->minTime;
  cur_index->maxTime = cur_index->maxTime;
  cur_index->id_bloom = storage::bloom_add(cur_index->id_bloom, ch->info->first.id);

  auto kv = std::make_pair(cur_index->maxTime, pos_index);
  _index->_itree.insert(kv);

  _openned_chunk.index = cur_index;
  _openned_chunk.pos = pos_index;
  // TODO restore this (flush)
  //  this->page_mmap->flush(get_header_offset(), sizeof(PageHeader));
  //  this->mmap->flush(get_index_offset() + sizeof(Page_ChunkIndex),
  //                    sizeof(Page_ChunkIndex));
  //  auto offset = get_chunks_offset(header->chunk_per_storage) +
  //                size_t(this->chunks - index[pos_index].offset);
  //  this->mmap->flush(offset, sizeof(header->chunk_size));
}

bool Page::is_full() const {
  return this->_free_poses.empty() &&
         (_openned_chunk.ch == nullptr || _openned_chunk.ch->is_full());
}

void Page::dec_reader() {
  boost::upgrade_lock<boost::shared_mutex> lg(_locker);
  header->count_readers--;
}

bool dariadb::storage::Page::minMaxTime(dariadb::Id, dariadb::Time *, dariadb::Time *) {
  return false;
}

ChunkLinkList dariadb::storage::Page::chunksByIterval(const QueryInterval &query) {
  return _index->get_chunks_links(query.ids, query.from, query.to, query.flag);
}

dariadb::Meas::Id2Meas Page::valuesBeforeTimePoint(const QueryTimePoint &q) {
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
    auto ptr_to_chunk_info_raw = reinterpret_cast<ChunkIndexInfo *>(ptr_to_begin);
    auto ptr_to_buffer_raw = ptr_to_begin + sizeof(ChunkIndexInfo);

    Chunk_Ptr ptr = nullptr;
    if (ptr_to_chunk_info_raw->is_zipped) {
      ptr = Chunk_Ptr{new ZippedChunk(ptr_to_chunk_info_raw, ptr_to_buffer_raw)};
    } else {
      // TODO implement not zipped page.
      assert(false);
    }
    Chunk_Ptr c{ptr};
    auto reader = c->get_reader();
    while (!reader->is_end()) {
      auto m = reader->readNext();
      if (m.time <= q.time_point && m.inQuery(q.ids, q.flag)) {
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

Cursor_ptr Page::readLinks(const ChunkLinkList &links) {
  auto raw_ptr = new PageCursor(this, links);
  Cursor_ptr result{raw_ptr};

  header->count_readers++;

  return result;
}

// class CountOfIdCallback : public Cursor::Callback {
// public:
//  dariadb::IdSet ids;
//  CountOfIdCallback() {}
//  ~CountOfIdCallback() {}
//
//  virtual void call(Chunk_Ptr &ptr) override {
//    if (ptr != nullptr) {
//      ids.insert(ptr->info->first.id);
//    }
//  }
//};

dariadb::append_result dariadb::storage::Page::append(const Meas &value) {
  if (add_to_target_chunk(value)) {
    return dariadb::append_result(1, 0);
  } else {
    return dariadb::append_result(0, 1);
  }
}

void dariadb::storage::Page::flush() {}
