#include "page.h"
#include "bloom_filter.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <fstream>

using namespace dariadb::storage;

class PageCursor : public dariadb::storage::Cursor {
public:
  PageCursor(Page *page, const dariadb::IdArray &ids, dariadb::Time from,
             dariadb::Time to, dariadb::Flag flag)
      : link(page), _ids(ids), _from(from), _to(to), _flag(flag) {
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
    if (read_poses.empty()) {
      _is_end = true;
      return;
    }
    auto _index_it = this->link->index[read_poses.front()];
    read_poses.pop_front();
    for (; !_is_end;) {
      if (_is_end) {
        Chunk_Ptr empty;
        cbk->call(empty);
        _is_end = true;
        break;
      }

      if (!dariadb::storage::bloom_check(_index_it.flag_bloom, _flag)) {
        if (!read_poses.empty()) {
          _index_it = this->link->index[read_poses.front()];
          read_poses.pop_front();
        } else {
          break;
        }
        continue;
      }

      if (check_index_rec(_index_it)) {
        auto ptr_to_begin = link->chunks + _index_it.offset;
        auto ptr_to_chunk_info =
            reinterpret_cast<ChunkIndexInfo *>(ptr_to_begin);
        auto ptr_to_buffer = ptr_to_begin + sizeof(ChunkIndexInfo);
        Chunk_Ptr ptr = nullptr;
        if (ptr_to_chunk_info->is_zipped) {
          // PM
          ptr = Chunk_Ptr{new ZippedChunk(ptr_to_chunk_info, ptr_to_buffer)};
        } else {
          // TODO implement not zipped page.
          assert(false);
        }
        Chunk_Ptr c{ptr};
        assert(c->info->last.time != 0);
        cbk->call(c);
        break;
      } else { // end of data;
        _is_end = true;
        Chunk_Ptr empty;
        cbk->call(empty);
        break;
      }
    }
    if (read_poses.empty()) {
      _is_end = true;
      return;
    }
  }

  bool check_index_rec(Page_ChunkIndex &it) const {
    return ((dariadb::utils::inInterval(_from, _to, it.minTime)) ||
            (dariadb::utils::inInterval(_from, _to, it.maxTime)));
  }

  void reset_pos() override { // start read from begining;
    _is_end = false;
    this->read_poses.clear();
    // TODO lock this->link; does'n need when call from ctor.
    for (auto i : _ids) {
      auto fres = this->link->_mtree.find(i);
      if (fres == this->link->_mtree.end()) {
        continue;
      }
      auto sz = fres->second.size();
      if (sz == size_t(0)) {
        continue;
      }

      auto it_to = fres->second.upper_bound(this->_to);
      auto it_from = fres->second.lower_bound(this->_from);

      if (it_from != fres->second.begin()) {
        if (it_from->first != this->_from) {
          --it_from;
        }
      }
      for (auto it = it_from; it != it_to; ++it) {
        this->read_poses.push_back(it->second);
      }
    }
    if (read_poses.empty()) {
      _is_end = true;
    }
  }

protected:
  Page *link;
  bool _is_end;
  dariadb::IdArray _ids;
  dariadb::Time _from, _to;
  dariadb::Flag _flag;
  std::mutex _locker;
  std::list<uint32_t> read_poses;
};

Page::~Page() {
  if (!iheader->is_sorted) {
    size_t pos = 0; // TODO crash safety
    Page_ChunkIndex *new_index =
        new Page_ChunkIndex[iheader->chunk_per_storage];
    memset(new_index, 0, sizeof(Page_ChunkIndex) * iheader->chunk_per_storage);

    for (auto it = _itree.begin(); it != _itree.end(); ++it, ++pos) {
      new_index[pos] = index[it->second];
    }
    memcpy(index, new_index,
           sizeof(Page_ChunkIndex) * iheader->chunk_per_storage);
    delete[] new_index;
    iheader->is_sorted = true;
  }
  _mtree.clear();
  _itree.clear();
  region = nullptr;
  header = nullptr;
  index = nullptr;
  chunks = nullptr;
  page_mmap->close();
  index_mmap->close();
}

uint64_t index_file_size(uint32_t chunk_per_storage) {
  return chunk_per_storage * sizeof(Page_ChunkIndex) + sizeof(IndexHeader);
}

Page *Page::create(std::string file_name, uint64_t sz,
                   uint32_t chunk_per_storage, uint32_t chunk_size) {
  auto res = new Page;
  auto mmap = utils::fs::MappedFile::touch(file_name, sz);
  auto region = mmap->data();
  std::fill(region, region + sz, 0);

  auto immap = utils::fs::MappedFile::touch(file_name + "i",
                                            index_file_size(chunk_per_storage));
  auto iregion = immap->data();
  std::fill(iregion, iregion + index_file_size(chunk_per_storage), 0);

  res->page_mmap = mmap;
  res->index_mmap = immap;
  res->region = region;
  res->iregion = iregion;

  res->header = reinterpret_cast<PageHeader *>(region);
  res->chunks = reinterpret_cast<uint8_t *>(region + sizeof(PageHeader));

  res->header->chunk_per_storage = chunk_per_storage;
  res->header->chunk_size = chunk_size;

  res->iheader = reinterpret_cast<IndexHeader *>(iregion);
  res->index =
      reinterpret_cast<Page_ChunkIndex *>(iregion + sizeof(IndexHeader));

  res->iheader->maxTime = std::numeric_limits<dariadb::Time>::min();
  res->iheader->minTime = std::numeric_limits<dariadb::Time>::max();
  res->iheader->chunk_per_storage = chunk_per_storage;
  res->iheader->chunk_size = chunk_size;
  res->iheader->is_sorted = false;

  for (uint32_t i = 0; i < res->header->chunk_per_storage; ++i) {
    res->_free_poses.push_back(i);
  }
  return res;
}

size_t get_header_offset() {
  return 0;
}

size_t get_index_offset() {
  return sizeof(PageHeader);
}

size_t get_chunks_offset() {
  return sizeof(PageHeader);
}

Page *Page::open(std::string file_name) {
  auto res = new Page;
  auto mmap = utils::fs::MappedFile::open(file_name);
  auto region = mmap->data();

  auto immap = utils::fs::MappedFile::open(file_name + "i");
  auto iregion = immap->data();

  res->page_mmap = mmap;
  res->index_mmap = immap;
  res->region = region;
  res->iregion = iregion;

  res->header = reinterpret_cast<PageHeader *>(region);
  res->chunks = reinterpret_cast<uint8_t *>(region + sizeof(PageHeader));

  res->iheader = reinterpret_cast<IndexHeader *>(iregion);
  res->index =
      reinterpret_cast<Page_ChunkIndex *>(iregion + sizeof(IndexHeader));

  if (res->header->chunk_size == 0) {
    throw MAKE_EXCEPTION("(res->header->chunk_size == 0)");
  }

  for (uint32_t i = 0; i < res->header->chunk_per_storage; ++i) {
    auto irec = &res->index[i];
    if (!irec->is_init) {
      res->_free_poses.push_back(i);
    } else {
      auto kv = std::make_pair(irec->maxTime, i);
      res->_itree.insert(kv);
      res->_mtree[irec->first.id].insert(kv);
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
  std::lock_guard<std::mutex> lg(_locker);
  if (is_full()) {
    header->is_full = true;
    return false;
  }
  auto byte_it = this->chunks;
  auto step = this->header->chunk_size + sizeof(ChunkIndexInfo);
  auto end = this->chunks + this->header->chunk_per_storage * step;
  while (true) {
    if (byte_it == end) {
      header->is_full = true;
      break;
    }
    ChunkIndexInfo *info = reinterpret_cast<ChunkIndexInfo *>(byte_it);
    if (!info->is_not_free) {
      auto ptr_to_begin = byte_it;
      auto ptr_to_buffer = ptr_to_begin + sizeof(ChunkIndexInfo);
      Chunk_Ptr ptr = nullptr;
      ptr = Chunk_Ptr{
          new ZippedChunk(info, ptr_to_buffer, header->chunk_size, m)};
      init_chunk_index_rec(ptr, ptr_to_begin);
      return true;
    } else {
      if ((info->first.id == m.id) && (!info->is_readonly)) {
        auto ptr_to_begin = byte_it;
        auto ptr_to_buffer = ptr_to_begin + sizeof(ChunkIndexInfo);
        Chunk_Ptr ptr = nullptr;
        if (info->is_zipped) {
          ptr = Chunk_Ptr{new ZippedChunk(info, ptr_to_buffer)};
          if (ptr->append(m)) {
            update_chunk_index_rec(ptr);
            return true;
          }
        } else {
          assert(false);
        }
      }
    }
    byte_it += step;
  }
  header->is_full = true;
  return false;
}
void Page::update_chunk_index_rec(const Chunk_Ptr &ptr) {
  for (size_t i = 0; i < header->addeded_chunks; ++i) {
    auto cur_index = &index[i];
    if (cur_index->first.id == ptr->info->first.id) {
      cur_index->last = ptr->info->last;
      return;
    }
  }
  assert(false);
}

void Page::init_chunk_index_rec(Chunk_Ptr ch, uint8_t *addr) {
  auto index_rec = ch->info;
  // auto buffer = ch->_buffer_t;

  assert(header->chunk_size == ch->info->size);

  uint32_t pos_index = 0;

  pos_index = _free_poses.front();
  _free_poses.pop_front();

  auto cur_index = &index[pos_index];
  cur_index->first = ch->info->first;
  cur_index->last = ch->info->last;

  cur_index->flag_bloom = ch->info->flag_bloom;
  cur_index->is_readonly = ch->info->is_readonly;
  cur_index->is_init = true;

  cur_index->offset = header->pos;
  header->pos += header->chunk_size + sizeof(ChunkIndexInfo);
  header->addeded_chunks++;
  auto kv = std::make_pair(index_rec->maxTime, pos_index);
  _itree.insert(kv);
  _mtree[index_rec->first.id].insert(kv);

  iheader->minTime = std::min(iheader->minTime, ch->info->minTime);
  iheader->maxTime = std::max(iheader->maxTime, ch->info->maxTime);
  cur_index->minTime = std::min(cur_index->minTime, ch->info->minTime);
  cur_index->maxTime = std::max(cur_index->maxTime, ch->info->maxTime);

  // TODO restore this (flush)
  //  this->page_mmap->flush(get_header_offset(), sizeof(PageHeader));
  //  this->mmap->flush(get_index_offset() + sizeof(Page_ChunkIndex),
  //                    sizeof(Page_ChunkIndex));
  //  auto offset = get_chunks_offset(header->chunk_per_storage) +
  //                size_t(this->chunks - index[pos_index].offset);
  //  this->mmap->flush(offset, sizeof(header->chunk_size));
}

bool Page::is_full() const {
  return this->_free_poses.empty();
}

Cursor_ptr Page::get_chunks(const dariadb::IdArray &ids, dariadb::Time from,
                            dariadb::Time to, dariadb::Flag flag) {
  std::lock_guard<std::mutex> lg(_locker);

  auto raw_ptr = new PageCursor(this, ids, from, to, flag);
  Cursor_ptr result{raw_ptr};

  header->count_readers++;

  return result;
}
/*
ChunksList Page::get_open_chunks() {
  std::lock_guard<std::mutex> lg(_locker);
  auto index_end = this->index + this->header->chunk_per_storage;
  auto index_it = this->index;
  ChunksList result;
  for (uint32_t pos = 0; index_it != index_end; ++index_it, ++pos) {
    if (!index_it->is_init) {
      continue;
    }
    if (!index_it->is_readonly) {
      index_it->is_init = false;
      auto ptr_to_begin = this->chunks + index_it->offset;
      auto ptr_to_chunk_info = reinterpret_cast<ChunkIndexInfo *>(ptr_to_begin);
      auto ptr_to_buffer = ptr_to_begin + sizeof(ChunkIndexInfo);
      Chunk_Ptr ptr = nullptr;
      if (ptr_to_chunk_info->is_zipped) {
          ptr = Chunk_Ptr{new ZippedChunk(ptr_to_chunk_info, ptr_to_buffer,
header->chunk_size)};
      } else {
        // TODO implement not zipped page.
        assert(false);
      }

      result.push_back(ptr);
      index_it->is_init = false;
      this->header->addeded_chunks--;
      _free_poses.push_back(pos);
    }
  }
  return result;
}
*/
void Page::dec_reader() {
  std::lock_guard<std::mutex> lg(_locker);
  header->count_readers--;
}

bool dariadb::storage::Page::minMaxTime(dariadb::Id id,
                                        dariadb::Time *minResult,
                                        dariadb::Time *maxResult) {
  std::lock_guard<std::mutex> lg(_locker);
  auto fres = _mtree.find(id);
  if (fres == _mtree.end()) {
    return false;
  }

  if (fres->second.size() == size_t(0)) {
    return false;
  }

  auto it_to = fres->second.end();
  auto it_from = fres->second.begin();
  if (fres->second.size() == size_t(1)) {
    it_to = it_from;
  } else {
    --it_to;
  }
  auto index_rec = this->index[it_from->second];
  *minResult = index_rec.minTime;
  index_rec = this->index[it_to->second];
  *maxResult = index_rec.maxTime;
  return true;
}

Cursor_ptr dariadb::storage::Page::chunksByIterval(const QueryInterval &query) {
  IdArray id_a = query.ids;
  if (id_a.empty()) {
    id_a = this->getIds();
  }
  return get_chunks(id_a, query.from, query.to, query.flag);
}

IdToChunkMap
dariadb::storage::Page::chunksBeforeTimePoint(const QueryTimePoint &q) {
  IdToChunkMap result;

  IdArray id_a = q.ids;
  if (id_a.empty()) {
    id_a = getIds();
  }

  ChunksList ch_list;
  auto cursor = this->get_chunks(id_a, iheader->minTime, q.time_point, q.flag);
  if (cursor == nullptr) {
    return result;
  }
  cursor->readAll(&ch_list);

  for (auto &v : ch_list) {
    auto find_res = result.find(v->info->first.id);
    if (find_res == result.end()) {
      result.insert(std::make_pair(v->info->first.id, v));
    } else {
      if (find_res->second->info->maxTime < v->info->maxTime) {
        result[v->info->first.id] = v;
      }
    }
  }
  return result;
}

class CountOfIdCallback : public Cursor::Callback {
public:
  dariadb::IdSet ids;
  CountOfIdCallback() {}
  ~CountOfIdCallback() {}

  virtual void call(Chunk_Ptr &ptr) override {
    if (ptr != nullptr) {
      ids.insert(ptr->info->first.id);
    }
  }
};

dariadb::IdArray dariadb::storage::Page::getIds() {
  dariadb::IdSet ss;
  for (auto kv : _mtree) {
    ss.insert(kv.first);
  }
  return dariadb::IdArray{ss.begin(), ss.end()};
}

dariadb::append_result dariadb::storage::Page::append(const Meas &value) {
  if (add_to_target_chunk(value)) {
    return dariadb::append_result(1, 0);
  } else {
    return dariadb::append_result(0, 1);
  }
}

void dariadb::storage::Page::flush() {}
