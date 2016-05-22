#include "page.h"
#include "bloom_filter.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <fstream>

using namespace dariadb::storage;

class PageLinksCursor {
public:
  PageLinksCursor(Page *page, const dariadb::IdArray &ids, dariadb::Time from,
                  dariadb::Time to, dariadb::Flag flag)
      : link(page), _ids(ids), _from(from), _to(to), _flag(flag) {
    reset_pos();
  }

  ~PageLinksCursor() {
    if (link != nullptr) {
      link = nullptr;
    }
  }

  bool is_end() const { return _is_end; }

  void readNext() {
    if (read_poses.empty()) {
      _is_end = true;
      return;
    }
    auto current_pos = read_poses.front();
    auto _index_it = this->link->index[read_poses.front()];
    read_poses.pop_front();
    for (; !_is_end;) {
      if (_is_end) {
        _is_end = true;
        break;
      }

      /*if (!dariadb::storage::bloom_check(_index_it.flag_bloom, _flag)) {
              if (!read_poses.empty()) {
                      _index_it = this->link->index[read_poses.front()];
                      current_pos = read_poses.front();
                      read_poses.pop_front();
              }
              else {
                      break;
              }
              continue;
      }

      if (check_index_rec(_index_it)) */
      {
        auto ptr_to_begin = link->chunks + _index_it.offset;
        auto ptr_to_chunk_info_raw = reinterpret_cast<ChunkIndexInfo *>(ptr_to_begin);

        ChunkLink sub_result;
        sub_result.id = ptr_to_chunk_info_raw->id;
        sub_result.pos = current_pos;
        sub_result.maxTime = ptr_to_chunk_info_raw->maxTime;
        sub_result.first_id = ptr_to_chunk_info_raw->first.id;
        this->resulted_links.push_back(sub_result);
        break;
      }
      /*else { break; }*/
    }
    if (read_poses.empty()) {
      _is_end = true;
      return;
    }
  }

  bool check_index_rec(Page_ChunkIndex &it) const {
    return ((dariadb::utils::inInterval(_from, _to, it.minTime)) ||
            (dariadb::utils::inInterval(_from, _to, it.maxTime))) ||
           (dariadb::utils::inInterval(it.minTime, it.maxTime, _from) ||
            dariadb::utils::inInterval(it.minTime, it.maxTime, _to));
  }

  void reset_pos() { // start read from begining;
    _is_end = false;
    this->read_poses.clear();

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
      if ((it_to == it_from) && (it_to != fres->second.end())) {
        it_to++;
      }
      for (auto it = it_from; it != it_to; ++it) {
        auto _index_it = link->index[it->second];

        auto bloom_result=check_blooms(_index_it);

        if (bloom_result) {
          if (check_index_rec(_index_it)) {
            this->read_poses.push_back(it->second);
          }
        }
      }
    }
    if (read_poses.empty()) {
      _is_end = true;
    }
  }

  bool check_blooms(const Page_ChunkIndex&_index_it)const{
      auto id_bloom_result=false;
      for(size_t i=0;i<_ids.size();++i){
          if(dariadb::storage::bloom_check(_index_it.id_bloom, _ids[i])){
              id_bloom_result=true;
              break;
          }
      }
      auto flag_bloom_result=false;
      if (dariadb::storage::bloom_check(_index_it.flag_bloom, _flag)) {
          flag_bloom_result=true;
      }
      return id_bloom_result&&flag_bloom_result;
  }

  ChunkLinkList resulted_links;

protected:
  Page *link;
  bool _is_end;
  dariadb::IdArray _ids;
  dariadb::Time _from, _to;
  dariadb::Flag _flag;
  std::list<uint32_t> read_poses;
};

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
    auto _index_it = this->link->index[_ch_links_iterator->pos];
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
  if (!readonly && !iheader->is_sorted) {
    size_t pos = 0; // TODO crash safety
    Page_ChunkIndex *new_index = new Page_ChunkIndex[iheader->chunk_per_storage];
    memset(new_index, 0, sizeof(Page_ChunkIndex) * iheader->chunk_per_storage);

    for (auto it = _itree.begin(); it != _itree.end(); ++it, ++pos) {
      new_index[pos] = index[it->second];
    }
    memcpy(index, new_index, sizeof(Page_ChunkIndex) * iheader->chunk_per_storage);
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

Page *Page::create(std::string file_name, uint64_t sz, uint32_t chunk_per_storage,
                   uint32_t chunk_size) {
  auto res = new Page;
  res->readonly = false;
  auto mmap = utils::fs::MappedFile::touch(file_name, sz);
  res->filename = file_name;
  auto region = mmap->data();
  std::fill(region, region + sz, 0);

  auto immap =
      utils::fs::MappedFile::touch(file_name + "i", index_file_size(chunk_per_storage));
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
  res->index = reinterpret_cast<Page_ChunkIndex *>(iregion + sizeof(IndexHeader));

  res->iheader->maxTime = dariadb::MIN_TIME;
  res->iheader->minTime = dariadb::MAX_TIME;
  res->iheader->chunk_per_storage = chunk_per_storage;
  res->iheader->chunk_size = chunk_size;
  res->iheader->is_sorted = false;
  res->iheader->id_bloom = storage::bloom_empty<dariadb::Id>();

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

Page *Page::open(std::string file_name, bool read_only) {
  auto res = new Page;
  res->readonly = read_only;
  auto mmap = utils::fs::MappedFile::open(file_name);
  res->filename = file_name;
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
  res->index = reinterpret_cast<Page_ChunkIndex *>(iregion + sizeof(IndexHeader));

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
      res->_mtree[irec->meas_id].insert(kv);
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


  if (_openned_chunk.ch!=nullptr && !_openned_chunk.ch->is_full()) {
    if (_openned_chunk.ch->append(m)) {
      update_index_info(_openned_chunk.index, _openned_chunk.pos, _openned_chunk.ch, m);
      return true;
    }
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
    if (!info->is_init) {
      auto ptr_to_begin = byte_it;
      auto ptr_to_buffer = ptr_to_begin + sizeof(ChunkIndexInfo);
      Chunk_Ptr ptr = nullptr;
      ptr = Chunk_Ptr{new ZippedChunk(info, ptr_to_buffer, header->chunk_size, m)};

      this->header->max_chunk_id++;
      ptr->info->id = this->header->max_chunk_id;
      _openned_chunk.ch = ptr;

      init_chunk_index_rec(ptr, ptr_to_begin);
      return true;
    } else {
      if ((!info->is_readonly)) {
        Chunk_Ptr ptr = read_chunk_from_ptr(byte_it);
        if (ptr->append(m)) {
          _openned_chunk.ch = ptr;
          update_chunk_index_rec(ptr, m);
          return true;
        }
      }
    }
    byte_it += step;
  }
  header->is_full = true;
  return false;
}

Chunk_Ptr Page::read_chunk_from_ptr(const uint8_t *ptr) const {
  auto non_const_ptr = const_cast<uint8_t *>(ptr);
  ChunkIndexInfo *info = reinterpret_cast<ChunkIndexInfo *>(non_const_ptr);
  auto ptr_to_begin = non_const_ptr;
  auto ptr_to_buffer = ptr_to_begin + sizeof(ChunkIndexInfo);
  if (info->is_zipped) {
    return Chunk_Ptr{new ZippedChunk(info, ptr_to_buffer)};
  } else {
    assert(false);
  }
  return nullptr;
}

void Page::update_chunk_index_rec(const Chunk_Ptr &ptr, const dariadb::Meas &m) {
  for (size_t i = 0; i < header->addeded_chunks; ++i) {
    auto cur_index = &index[i];
    if ((cur_index->meas_id == ptr->info->first.id) &&
        (cur_index->meas_id == ptr->info->first.time)) {
      update_index_info(cur_index, i, ptr, m);
      return;
    }
  }
  assert(false);
}

void Page::update_index_info(Page_ChunkIndex *cur_index, const uint32_t pos,
                             const Chunk_Ptr &ptr, const dariadb::Meas &m) {
  // cur_index->last = ptr->info->last;
  iheader->id_bloom = storage::bloom_add(iheader->id_bloom, ptr->info->first.id);
  iheader->minTime = std::min(iheader->minTime, ptr->info->minTime);
  iheader->maxTime = std::max(iheader->maxTime, ptr->info->maxTime);

  for (auto it = _itree.lower_bound(cur_index->maxTime);
       it != _itree.upper_bound(cur_index->maxTime); ++it) {
    if ((it->first == cur_index->maxTime) && (it->second == pos)) {
      _itree.erase(it);
      break;
    }
  }

  auto tree = &_mtree[cur_index->meas_id];
  auto from = tree->lower_bound(cur_index->maxTime);
  auto to = tree->upper_bound(cur_index->maxTime);
  for (auto it = from; it != to; ++it) {
    if ((it->second == pos) && (it->first == cur_index->maxTime)) {
      tree->erase(it);
      break;
    }
  }

  cur_index->minTime = std::min(cur_index->minTime, m.time);
  cur_index->maxTime = std::max(cur_index->maxTime, m.time);
  cur_index->flag_bloom = ptr->info->flag_bloom;
  cur_index->id_bloom = ptr->info->id_bloom;
  auto kv = std::make_pair(cur_index->maxTime, pos);
  _itree.insert(kv);
  tree->insert(kv);
}

void Page::init_chunk_index_rec(Chunk_Ptr ch, uint8_t *addr) {
  auto index_rec = ch->info;
  // auto buffer = ch->_buffer_t;

  assert(header->chunk_size == ch->info->size);

  uint32_t pos_index = 0;

  pos_index = _free_poses.front();
  _free_poses.pop_front();

  auto cur_index = &index[pos_index];
  cur_index->chunk_id = ch->info->id;
  cur_index->meas_id = ch->info->first.id;
  // cur_index->last = ch->info->last;

  cur_index->flag_bloom = ch->info->flag_bloom;
  cur_index->id_bloom = ch->info->id_bloom;
  cur_index->is_readonly = ch->info->is_readonly;
  cur_index->is_init = true;

  cur_index->offset = header->pos;

  cur_index->minTime = dariadb::MAX_TIME;
  cur_index->maxTime = dariadb::MIN_TIME;

  header->pos += header->chunk_size + sizeof(ChunkIndexInfo);
  header->addeded_chunks++;

  auto kv = std::make_pair(index_rec->maxTime, pos_index);
  _itree.insert(kv);
  _mtree[index_rec->first.id].insert(kv);

  iheader->minTime = std::min(iheader->minTime, ch->info->minTime);
  iheader->maxTime = std::max(iheader->maxTime, ch->info->maxTime);
  iheader->id_bloom = storage::bloom_add(iheader->id_bloom, ch->info->first.id);

  cur_index->minTime = std::min(cur_index->minTime, ch->info->minTime);
  cur_index->maxTime = std::max(cur_index->maxTime, ch->info->maxTime);

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
  return this->_free_poses.empty();
}

ChunkLinkList Page::get_chunks_links(const dariadb::IdArray &ids, dariadb::Time from,
                                     dariadb::Time to, dariadb::Flag flag) {
  boost::shared_lock<boost::shared_mutex> lg(_locker);

  PageLinksCursor c(this, ids, from, to, flag);
  c.reset_pos();

  while (!c.is_end()) {
    c.readNext();
  }

  return c.resulted_links;
}

void Page::dec_reader() {
  boost::upgrade_lock<boost::shared_mutex> lg(_locker);
  header->count_readers--;
}

bool dariadb::storage::Page::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                                        dariadb::Time *maxResult) {
  boost::shared_lock<boost::shared_mutex> lg(_locker);
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

ChunkLinkList dariadb::storage::Page::chunksByIterval(const QueryInterval &query) {
  return get_chunks_links(query.ids, query.from, query.to, query.flag);
}

ChunkLinkList Page::chunksBeforeTimePoint(const QueryTimePoint &q) {
  ChunkLinkList result;
  auto raw_links = this->get_chunks_links(q.ids, iheader->minTime, q.time_point, q.flag);
  if (raw_links.empty()) {
    return result;
  }
  std::map<dariadb::Id, ChunkLink> id2link;
  for (auto &v : raw_links) {
    auto find_res = id2link.find(v.first_id);
    if (find_res == id2link.end()) {
      id2link.insert(std::make_pair(v.first_id, v));
    } else {
      if (find_res->second.maxTime < v.maxTime) {
        id2link[v.first_id] = v;
      }
    }
  }

  for (auto kv : id2link) {
    result.push_back(kv.second);
  }

  return result;
}

Cursor_ptr Page::readLinks(const ChunkLinkList &links) {
  auto raw_ptr = new PageCursor(this, links);
  Cursor_ptr result{raw_ptr};

  header->count_readers++;

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
