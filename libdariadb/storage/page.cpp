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

      if (!dariadb::storage::bloom_check(_index_it.info.flag_bloom, _flag)) {
        if (!read_poses.empty()) {
          _index_it = this->link->index[read_poses.front()];
          read_poses.pop_front();
        } else {
          break;
        }
        continue;
      }

      if (check_index_rec(_index_it)) {
        Chunk_Ptr ptr = nullptr;
        if (_index_it.info.is_zipped) {
          ptr = Chunk_Ptr{new ZippedChunk(_index_it.info,
                                          link->chunks + _index_it.offset,
                                          link->header->chunk_size)};
        } else {
          // TODO implement not zipped page.
          assert(false);
        }
        Chunk_Ptr c{ptr};
        assert(c->last.time != 0);
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
    return ((dariadb::utils::inInterval(_from, _to, it.info.minTime)) ||
            (dariadb::utils::inInterval(_from, _to, it.info.maxTime)));
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

    if(!iheader->is_sorted){
        size_t pos=0;//TODO crash safety
        Page_ChunkIndex* new_index=new Page_ChunkIndex[iheader->chunk_per_storage];
        memset(new_index,0,sizeof(Page_ChunkIndex)*iheader->chunk_per_storage);

        for(auto it=_itree.begin();it!=_itree.end();++it,++pos){
            new_index[pos]=index[it->second];
        }
        memcpy(index,new_index,sizeof(Page_ChunkIndex)*iheader->chunk_per_storage);
        delete[] new_index;
        iheader->is_sorted=true;
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

uint64_t index_file_size(uint32_t chunk_per_storage){
    return chunk_per_storage*sizeof(Page_ChunkIndex)+sizeof(IndexHeader);
}

Page *Page::create(std::string file_name, uint64_t sz,
                   uint32_t chunk_per_storage, uint32_t chunk_size, MODE mode) {
  auto res = new Page;
  auto mmap = utils::fs::MappedFile::touch(file_name, sz);
  auto region = mmap->data();
  std::fill(region, region + sz, 0);

  auto immap = utils::fs::MappedFile::touch(file_name+"i", index_file_size(chunk_per_storage));
  auto iregion = immap->data();
  std::fill(iregion, iregion + index_file_size(chunk_per_storage), 0);

  res->page_mmap = mmap;
  res->index_mmap = immap;
  res->region = region;
  res->iregion = iregion;

  res->header = reinterpret_cast<PageHeader*>(region);
  res->chunks = reinterpret_cast<uint8_t*>(region + sizeof(PageHeader));

  res->header->chunk_per_storage = chunk_per_storage;
  res->header->chunk_size = chunk_size;
  res->header->is_overwrite = false;
  res->header->mode = mode;

  res->iheader = reinterpret_cast<IndexHeader*>(iregion);
  res->index = reinterpret_cast<Page_ChunkIndex*>(iregion + sizeof(IndexHeader));

  res->iheader->maxTime = std::numeric_limits<dariadb::Time>::min();
  res->iheader->minTime = std::numeric_limits<dariadb::Time>::max();
  res->iheader->chunk_per_storage = chunk_per_storage;
  res->iheader->chunk_size = chunk_size;
  res->iheader->is_sorted=false;

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

  auto immap = utils::fs::MappedFile::open(file_name+"i");
  auto iregion = immap->data();

  res->page_mmap = mmap;
  res->index_mmap = immap;
  res->region = region;
  res->iregion = iregion;

  res->header = reinterpret_cast<PageHeader*>(region);
  res->chunks = reinterpret_cast<uint8_t*>(region + sizeof(PageHeader));

  res->iheader = reinterpret_cast<IndexHeader*>(iregion);
  res->index = reinterpret_cast<Page_ChunkIndex*>(iregion + sizeof(IndexHeader));

  if (res->header->chunk_size == 0) {
    throw MAKE_EXCEPTION("(res->header->chunk_size == 0)");
  }

  for (uint32_t i = 0; i < res->header->chunk_per_storage; ++i) {
    auto irec = &res->index[i];
    if (!irec->is_init) {
      res->_free_poses.push_back(i);
    } else {
      auto kv = std::make_pair(irec->info.maxTime, i);
      res->_itree.insert(kv);
      res->_mtree[irec->info.first.id].insert(kv);
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

IndexHeader Page::readIndexHeader(std::string ifile){
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

bool Page::append(const ChunksList &ch) {
  for (auto &c : ch) {
    if (!this->append(c)) {
      return false;
    }
  }
  return true;
}

bool Page::append(const Chunk_Ptr &ch) {
  std::lock_guard<std::mutex> lg(_locker);
  auto index_rec = (ChunkIndexInfo *)ch.get();
  auto buffer = ch->_buffer_t;

  assert(ch->last.time != 0);
  assert(header->chunk_size == ch->_size);
  uint32_t pos_index = 0;
  dariadb::Id removedId{0}; /// need to save overwriten reccord;
  if (is_full()) {
    if (header->mode == MODE::SINGLE) {
      /// get oldes index reccord.
      header->is_overwrite = true;
      pos_index = this->_itree.begin()->second;
      removedId = index[pos_index].info.first.id;
    } else {
      return false;
    }
  } else {
    pos_index = _free_poses.front();
    _free_poses.pop_front();
  }
  index[pos_index].info = *index_rec;
  index[pos_index].is_init = true;

  if (!header->is_overwrite) {
    index[pos_index].offset = header->pos;
    header->pos += header->chunk_size;
    header->addeded_chunks++;
    auto kv = std::make_pair(index_rec->maxTime, pos_index);
    _itree.insert(kv);
    _mtree[index_rec->first.id].insert(kv);
  } else {
    auto it = this->_itree.begin();
    auto removedTime = it->first;

    /// remove from tree and multitree;
    auto fres = _mtree.find(removedId);
    assert(fres != _mtree.end());
    auto mtr_it = fres->second.find(removedTime);
    _itree.erase(it);
    fres->second.erase(mtr_it);
    /// insert new {key,value}
    auto kv = std::make_pair(index_rec->maxTime, pos_index);
    _itree.insert(kv);
    _mtree[index_rec->first.id].insert(kv);
  }
  memcpy(this->chunks + index[pos_index].offset, buffer,
         sizeof(uint8_t) * header->chunk_size);

  iheader->minTime = std::min(iheader->minTime, ch->minTime);
  iheader->maxTime = std::max(iheader->maxTime, ch->maxTime);

  //TODO uncomment this
//  this->page_mmap->flush(get_header_offset(), sizeof(PageHeader));
//  this->mmap->flush(get_index_offset() + sizeof(Page_ChunkIndex),
//                    sizeof(Page_ChunkIndex));
//  auto offset = get_chunks_offset(header->chunk_per_storage) +
//                size_t(this->chunks - index[pos_index].offset);
//  this->mmap->flush(offset, sizeof(header->chunk_size));
  return true;
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

ChunksList Page::get_open_chunks() {
  std::lock_guard<std::mutex> lg(_locker);
  auto index_end = this->index + this->header->chunk_per_storage;
  auto index_it = this->index;
  ChunksList result;
  for (uint32_t pos = 0; index_it != index_end; ++index_it, ++pos) {
    if (!index_it->is_init) {
      continue;
    }
    if (!index_it->info.is_readonly) {
      index_it->is_init = false;
      Chunk *ptr = nullptr;
      if (index_it->info.is_zipped) {
        ptr = new ZippedChunk(index_it->info, this->chunks + index_it->offset,
                              this->header->chunk_size);
      } else {
        // TODO implement not zipped chunk
        assert(false);
      }
      Chunk_Ptr c = Chunk_Ptr(ptr);
      result.push_back(c);
      index_it->is_init = false;
      this->header->addeded_chunks--;
      _free_poses.push_back(pos);
    }
  }
  return result;
}

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
  *minResult = index_rec.info.minTime;
  index_rec = this->index[it_to->second];
  *maxResult = index_rec.info.maxTime;
  return true;
}

Cursor_ptr dariadb::storage::Page::chunksByIterval(const IdArray &ids,
                                                   Flag flag, Time from,
                                                   Time to) {
  IdArray id_a = ids;
  if (id_a.empty()) {
    id_a = this->getIds();
  }
  return get_chunks(id_a, from, to, flag);
}

IdToChunkMap dariadb::storage::Page::chunksBeforeTimePoint(const IdArray &ids,
                                                           Flag flag,
                                                           Time timePoint) {
  IdToChunkMap result;

  IdArray id_a = ids;
  if (id_a.empty()) {
    id_a = getIds();
  }

  ChunksList ch_list;
  auto cursor = this->get_chunks(id_a, iheader->minTime, timePoint, flag);
  if (cursor == nullptr) {
    return result;
  }
  cursor->readAll(&ch_list);

  for (auto &v : ch_list) {
    auto find_res = result.find(v->first.id);
    if (find_res == result.end()) {
      result.insert(std::make_pair(v->first.id, v));
    } else {
      if (find_res->second->maxTime < v->maxTime) {
        result[v->first.id] = v;
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
      ids.insert(ptr->first.id);
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
