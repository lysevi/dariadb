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

void Page::init_header() {
  this->readonly = false;
  this->header->minTime = dariadb::MAX_TIME;
  this->header->maxTime = dariadb::MIN_TIME;
  this->header->is_closed = false;
  this->header->is_open_to_write = true;
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
                                  index_file_size(chunk_per_storage));
  res->region = region;

  res->header = reinterpret_cast<PageHeader *>(region);
  res->chunks = reinterpret_cast<uint8_t *>(region + sizeof(PageHeader));
  res->init_header();
  res->header->chunk_per_storage = chunk_per_storage;
  res->header->chunk_size = chunk_size;
  res->page_mmap->flush(0, sizeof(PageHeader));
  return res;
}

Page *Page::create(std::string file_name, uint64_t chunk_id, uint32_t max_chunk_size, const Meas::MeasArray &ma){
    TIMECODE_METRICS(ctmd, "create", "Page::create(array)");

    dariadb::IdSet dropped;
    auto count=ma.size();
    std::vector<bool> visited(count);
    auto begin=ma.cbegin();
    auto end=ma.cend();
    size_t i = 0;
    std::list<Meas::MeasList> to_compress;
    for (auto it = begin; it != end; ++it, ++i) {
      if (visited[i]) {
        continue;
      }
      if (dropped.find(it->id) != dropped.end()) {
        continue;
      }
      Meas::MeasList current_id_values;
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
      to_compress.push_back(std::move(current_id_values));
    }


    PageHeader phdr;
    memset(&phdr, 0,sizeof(PageHeader));
    phdr.maxTime=dariadb::MIN_TIME;
    phdr.minTime=dariadb::MAX_TIME;
    phdr.max_chunk_id=chunk_id;

    using HdrAndBuffer=std::tuple<ChunkHeader,std::shared_ptr<uint8_t>>;
    std::list<HdrAndBuffer> results;

    for(auto&lst:to_compress){
        ChunkHeader hdr;
        memset(&hdr,0,sizeof(ChunkHeader));
        auto lst_size=lst.size();
        auto buff_size=lst_size*sizeof(Meas);
        std::shared_ptr<uint8_t> buffer_ptr{new uint8_t[buff_size]};
        ZippedChunk ch(&hdr, buffer_ptr.get(), buff_size,lst.front());
        lst.pop_front();
        for(auto&v:lst){
            ch.append(v);
        }
        ch.close();

        phdr.max_chunk_id++;
        phdr.minTime=std::min(phdr.minTime,ch.header->minTime);
        phdr.maxTime=std::max(phdr.maxTime,ch.header->maxTime);
        ch.header->id = phdr.max_chunk_id;

        HdrAndBuffer subres= std::make_tuple(hdr,buffer_ptr);
        results.push_back(subres);
    }

    auto file = std::fopen(file_name.c_str(), "ab");
    if (file == nullptr) {
      throw MAKE_EXCEPTION("aofile: append error.");
    }

    std::fwrite(&(phdr), sizeof(PageHeader), 1, file);
    uint64_t offset=0;

    for(auto hb:results){
        ChunkHeader chunk_header=std::get<0>(hb);
        std::shared_ptr<uint8_t> chunk_buffer_ptr=std::get<1>(hb);

        chunk_header.pos_in_page=phdr.addeded_chunks;
        phdr.addeded_chunks++;
        auto cur_chunk_buf_size=chunk_header.size - chunk_header.bw_pos+1;

        chunk_header.size=cur_chunk_buf_size;
        chunk_header.offset_in_page=offset;
        std::fwrite(&(chunk_header), sizeof(ChunkHeader), 1, file);
        std::fwrite(chunk_buffer_ptr.get()+(chunk_header.size-cur_chunk_buf_size),
                    sizeof(uint8_t),
                    cur_chunk_buf_size, file);


        offset+=sizeof(ChunkHeader) + cur_chunk_buf_size;

    }
    auto page_size=offset+sizeof(PageHeader);
    phdr.write_offset=offset;
    phdr.chunk_size=1;
    phdr.filesize=page_size;
    std::fclose(file);

    auto res = new Page;
    res->readonly = false;
    auto mmap = utils::fs::MappedFile::open(file_name, false);
    res->filename = file_name;
    auto region = mmap->data();

    res->page_mmap = mmap;
    res->_index =PageIndex::create(PageIndex::index_name_from_page_name(file_name),
                                   index_file_size(phdr.addeded_chunks));
    res->region = region;

    res->header = reinterpret_cast<PageHeader *>(region);
    *(res->header)=phdr;
    res->chunks = reinterpret_cast<uint8_t *>(region + sizeof(PageHeader));
    res->readonly = false;
    res->header->is_closed = true;
    res->header->is_open_to_write = false;
    res->page_mmap->flush(0, sizeof(PageHeader));
    res->update_index_recs();
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
  res->check_page_struct();
  return res;
}

void Page::check_page_struct() {
#ifdef DEBUG
  for (uint32_t i = 0; i < header->chunk_per_storage; ++i) {
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
  return PageIndex::readIndexHeader(ifile);
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
      if (!ptr->check_checksum()) {
        logger_fatal("fsck: remove broken chunk #"
                     << ptr->header->id << " id:" << ptr->header->first.id << " time: ["
                     << to_string(ptr->header->minTime) << " : "
                     << to_string(ptr->header->maxTime) << "]");
        mark_as_non_init(ptr);
      }
    }
    ++pos;
    byte_it += step;
  }
}

bool Page::add_to_target_chunk(const dariadb::Meas &m) {
  TIMECODE_METRICS(ctmd, "append", "Page::add_to_target_chunk");
  assert(!this->readonly);
  std::lock_guard<std::mutex> lg(_locker);
  if (is_full()) {
    header->is_full = true;
    _index->iheader->is_full = true;
    return false;
  }

  if (_openned_chunk.ch != nullptr && !_openned_chunk.ch->is_full()) {
    if (_openned_chunk.ch->header->last.id != m.id) {
      assert(_openned_chunk.ch->header->id == _openned_chunk.index->chunk_id);
      close_corrent_chunk();
    } else {
      if (_openned_chunk.ch->append(m)) {
        assert(_openned_chunk.ch->header->id == _openned_chunk.index->chunk_id);
        this->header->minTime = std::min(m.time, this->header->minTime);
        this->header->maxTime = std::max(m.time, this->header->maxTime);

        _index->update_index_info(_openned_chunk.index, _openned_chunk.ch, m,
                                  _openned_chunk.pos);
        return true;
      } else {
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

      _openned_chunk.ch = ptr;

      ptr->header->offset_in_page=header->write_offset;
      init_chunk_index_rec(ptr,this->header->addeded_chunks);
      return true;
    }
    byte_it += step;
  }
  header->is_full = true;
  _index->iheader->is_full = true;
  return false;
}

void Page::close_corrent_chunk() {
  if (_openned_chunk.ch != nullptr) {

    _openned_chunk.ch->close();
#ifdef ENABLE_METRICS
    // calc perscent of free space in closed chunks
    auto used_space = _openned_chunk.ch->header->size - _openned_chunk.ch->header->bw_pos;
    auto size = _openned_chunk.ch->header->size;
    auto percent = used_space * float(100.0) / size;
    auto raw_metric_ptr = new dariadb::utils::metrics::FloatMetric(percent);
    ADD_METRICS("write", "chunk_free",
                dariadb::utils::metrics::IMetric_Ptr{raw_metric_ptr});
#endif
    _openned_chunk.ch = nullptr;
  }
}

void Page::update_index_recs(){
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
        ptr = Chunk_Ptr{new ZippedChunk(info,ptr_to_buffer)};

        this->header->max_chunk_id++;
        ptr->header->id = this->header->max_chunk_id;

        init_chunk_index_rec(ptr, ptr->header->pos_in_page);
      }
      byte_it += sizeof(ChunkHeader)+info->size;
    }
    header->is_full = true;
    _index->iheader->is_full = true;
}

void Page::init_chunk_index_rec(Chunk_Ptr ch,uint32_t pos_index) {
  TIMECODE_METRICS(ctmd, "write", "Page::init_chunk_index_rec");

  auto cur_index = &_index->index[pos_index];
  assert(cur_index->chunk_id == 0);
  assert(cur_index->is_init == false);
  ch->header->pos_in_page = pos_index;
  cur_index->chunk_id = ch->header->id;
  cur_index->is_init = true;
  cur_index->offset = ch->header->offset_in_page;// header->write_offset;

  header->write_offset += header->chunk_size + sizeof(ChunkHeader);
  header->addeded_chunks++;

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

  _openned_chunk.index = cur_index;
  _openned_chunk.pos = pos_index;
}

bool Page::is_full() const {
  return this->header->addeded_chunks == this->header->chunk_per_storage &&
         (_openned_chunk.ch == nullptr || _openned_chunk.ch->is_full());
}

void Page::dec_reader() {
  std::lock_guard<std::mutex> lg(_locker);
  header->count_readers--;
}

bool Page::minMaxTime(dariadb::Id id, dariadb::Time *minTime,
                                        dariadb::Time *maxTime) {
  QueryInterval qi{dariadb::IdArray{id}, 0, this->header->minTime, this->header->maxTime};
  auto all_chunks = this->chunksByIterval(qi);

  bool result = false;
  if (!all_chunks.empty()) {
    result = true;
  }
  *minTime = dariadb::MAX_TIME;
  *maxTime = dariadb::MIN_TIME;
  for (auto &link : all_chunks) {
    auto _index_it = this->_index->index[link.offset];
    *minTime = std::min(*minTime, _index_it.minTime);
    *maxTime = std::max(*maxTime, _index_it.maxTime);
  }
  return result;
}

ChunkLinkList Page::chunksByIterval(const QueryInterval &query) {
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
    auto _index_it = this->_index->index[it->offset];
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
                     IReaderClb *clb) {
  TIMECODE_METRICS(ctmd, "readLinks", "Page::readLinks");
  std::lock_guard<std::mutex> lg(_locker);
  auto _ch_links_iterator = links.cbegin();
  if (_ch_links_iterator == links.cend()) {
    return;
  }

  for (; _ch_links_iterator != links.cend(); ++_ch_links_iterator) {
    auto _index_it = this->_index->index[_ch_links_iterator->offset];
    Chunk_Ptr search_res;

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
    } else {
      assert(false);
    }
    Chunk_Ptr c{ptr};
    search_res = c;

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

dariadb::append_result Page::append(const Meas &value) {
  TIMECODE_METRICS(ctmd, "append", "Page::append");
  if (add_to_target_chunk(value)) {
    return dariadb::append_result(1, 0);
  } else {
    return dariadb::append_result(0, 1);
  }
}

void Page::flush() {
  this->page_mmap->flush();
  this->_index->index_mmap->flush();
}

void Page::mark_as_init(Chunk_Ptr &ptr) {
  ptr->header->is_init = true;
  auto pos = ptr->header->pos_in_page;
  _index->index[pos].is_init = true;
  this->header->removed_chunks--;
}

void Page::mark_as_non_init(Chunk_Ptr &ptr) {
  ptr->header->is_init = false;
  auto pos = ptr->header->pos_in_page;
  _index->iheader->is_sorted = false;
  _index->index[pos].is_init = false;
  this->header->removed_chunks++;
}

std::list<Chunk_Ptr> Page::get_not_full_chunks() {
  auto step = this->header->chunk_size + sizeof(ChunkHeader);
  auto byte_it = this->chunks;
  auto end = this->chunks + this->header->addeded_chunks * step;
  size_t pos = 0;
  std::list<Chunk_Ptr> result;
  while (true) {
    if (byte_it == end) {
      break;
    }
    ChunkHeader *info = reinterpret_cast<ChunkHeader *>(byte_it);
    auto ptr_to_begin = byte_it;
    auto ptr_to_buffer = ptr_to_begin + sizeof(ChunkHeader);
    if (info->is_init && (info->bw_pos > 1)) {
      Chunk_Ptr ptr = nullptr;
      ptr = Chunk_Ptr{new ZippedChunk(info, ptr_to_buffer)};
      result.push_back(ptr);
    }
    ++pos;
    byte_it += step;
  }
  return result;
}

std::list<Chunk_Ptr> Page::chunks_by_pos(std::vector<uint32_t> poses) {
  std::list<Chunk_Ptr> result;
  for (auto &p : poses) {
    auto irec = this->_index->index[p];

    auto ptr_to_begin = this->chunks + irec.offset;
    ChunkHeader *info = reinterpret_cast<ChunkHeader *>(ptr_to_begin);
    auto ptr_to_buffer = ptr_to_begin + sizeof(ChunkHeader);
    Chunk_Ptr ptr = nullptr;
    ptr = Chunk_Ptr{new ZippedChunk(info, ptr_to_buffer)};
    result.push_back(ptr);
    assert(info->pos_in_page == p);
  }
  return result;
}
