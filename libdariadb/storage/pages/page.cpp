#ifdef MSVC
#define _CRT_SECURE_NO_WARNINGS // for fopen
#define _SCL_SECURE_NO_WARNINGS // for stx::btree in msvc build.
#endif
#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/callbacks.h>
#include <libdariadb/storage/cursors.h>
#include <libdariadb/storage/pages/helpers.h>
#include <libdariadb/storage/pages/page.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <libdariadb/utils/exception.h>
#include <algorithm>

#include <cstring>
#include <fstream>
#include <map>

using namespace dariadb::storage;
using namespace dariadb;

namespace page_utils {

MeasArray applyFilter(MeasArray &inputValues, ICompactionController *logic) {
  MeasArray result;
  MeasArray filtered;
  std::vector<int> data_filter(inputValues.size());
  std::fill_n(data_filter.begin(), data_filter.size(), int(1));
  ENSURE(data_filter.size() == inputValues.size());

  logic->compact(inputValues, data_filter);
  auto enabled_count = std::count_if(data_filter.begin(), data_filter.end(),
                                     [](const int v) { return v == int(1); });
  if (enabled_count != 0) {
    filtered.reserve(enabled_count);
    for (size_t i = 0; i < inputValues.size(); ++i) {
      if (data_filter[i] == uint8_t(1)) {
        filtered.push_back(inputValues[i]);
      }
    }
    result = std::move(filtered);
    std::sort(result.begin(), result.end(), meas_time_compare_less());
    ENSURE(result.empty() || result.front().time <= result.back().time);
  }
  return result;
}

void writeChunkToResultPage(std::unordered_map<std::string, ChunkLinkList> &fname2links,
                            std::unordered_map<std::string, Page_Ptr> &openned_pages,
                            PageFooter &phdr, IndexFooter &ihdr, FILE *out_file,
                            FILE *out_index_file) {
  for (auto f2l : fname2links) {
    auto p = openned_pages[f2l.first];
    auto chunk_callback = [&phdr, &ihdr, &out_index_file,
                           &out_file](const Chunk_Ptr &chunk) {
      // chunk->close();
      chunk->is_owner = false;
      if (!chunk->checkChecksum()) {
        THROW_EXCEPTION("checksum error");
      }
      auto hdr_ptr = chunk->header;
      PageInner::HdrAndBuffer hab;
      hab.buffer = boost::shared_array<uint8_t>(chunk->_buffer_t);
      hab.hdr = *(hdr_ptr);
      phdr.max_chunk_id++;
      hab.hdr.id = phdr.max_chunk_id;

      std::list<PageInner::HdrAndBuffer> compressed_results{hab};
      auto page_size = PageInner::writeToFile(out_file, out_index_file, phdr, ihdr,
                                              compressed_results, phdr.filesize);

      phdr.filesize = page_size;
      delete hdr_ptr;
      return false;
    };
    p->apply_to_chunks(f2l.second, chunk_callback);
  }
}
} // namespace page_utils

Page::Page(const PageFooter &ftr, std::string fname) : footer(ftr), filename(fname) {}

Page::~Page() {
  _index = nullptr;
}

struct page_create_write_description {
  PageFooter phdr;
  IndexFooter ihdr;
  page_create_write_description(uint16_t lvl, uint64_t chunk_id)
      : phdr(lvl, chunk_id), ihdr() {}
};

bool create_write_logic(
    std::shared_ptr<std::list<PageInner::HdrAndBuffer>> compressed_results, FILE *file,
    FILE *index_file, std::shared_ptr<page_create_write_description> d,
    const std::string &file_name, on_create_complete_callback on_complete) {
  if (!compressed_results->empty()) {
    std::list<PageInner::HdrAndBuffer> subresult;
    for (size_t i = 0; i < 10; ++i) {
      if (compressed_results->empty()) {
        break;
      }
      subresult.push_back(compressed_results->front());
      compressed_results->pop_front();
    }
    if (!subresult.empty()) {
      auto page_size = PageInner::writeToFile(file, index_file, d->phdr, d->ihdr,
                                              subresult, d->phdr.filesize);
      d->phdr.filesize = page_size;
    }
    if (!compressed_results->empty()) {
      return true;
    }
  }
  d->ihdr.level = d->phdr.level;
  ENSURE(memcmp(&d->phdr.stat, &d->ihdr.stat, sizeof(Statistic)) == 0);
  std::fwrite((char *)&d->phdr, sizeof(PageFooter), 1, file);
  std::fclose(file);

  std::fwrite(&d->ihdr, sizeof(IndexFooter), 1, index_file);
  std::fclose(index_file);
  auto result = Page::open(file_name, d->phdr);
  on_complete(result);
  return false;
}

void Page::create(const std::string &file_name, uint16_t lvl, uint64_t chunk_id,
                  uint32_t max_chunk_size, const SplitedById &to_compress,
                  on_create_complete_callback on_complete) {
  ENSURE(to_compress.size() == size_t(1));
  using namespace dariadb::utils::async;
  PageFooter phdr(lvl, chunk_id);

  auto compressed_results = PageInner::compressValues(to_compress, phdr, max_chunk_size);
  auto file = std::fopen(file_name.c_str(), "ab");
  if (file == nullptr) {
    THROW_EXCEPTION("file is null");
  }

  IndexFooter ihdr;

  auto index_file =
      std::fopen(PageIndex::index_name_from_page_name(file_name).c_str(), "ab");
  if (index_file == nullptr) {
    THROW_EXCEPTION("can`t open file ", file_name);
  }

  auto d = std::make_shared<page_create_write_description>(0, 0);
  d->phdr = phdr;

  AsyncTask at = [compressed_results, file, index_file, d, file_name,
                  on_complete](const ThreadInfo &ti) {
    return create_write_logic(compressed_results, file, index_file, d, file_name,
                              on_complete);
  };
  ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
}

Page_Ptr Page::repackTo(const std::string &file_name, uint16_t lvl, uint64_t chunk_id,
                        uint32_t max_chunk_size,
                        const std::list<std::string> &pages_full_paths,
                        ICompactionController *logic) {
  std::unordered_map<std::string, Page_Ptr> openned_pages;
  openned_pages.reserve(pages_full_paths.size());

  auto timepoint = dariadb::timeutil::current_time();

  std::map<uint64_t, ChunkLinkList> links;
  QueryInterval qi({}, 0, MIN_TIME, MAX_TIME);
  for (auto &p_full_path : pages_full_paths) {
    Page_Ptr p = Page::open(p_full_path);
    openned_pages.emplace(std::make_pair(p_full_path, p));

    auto clinks = p->linksByIterval(qi);
    for (auto &cl : clinks) {
      if (logic != nullptr && (timepoint - cl.maxTime >= logic->eraseOlderThan)) {
        continue;
      }
      cl.page_name = p_full_path;
      links[cl.meas_id].push_back(cl);
    }
  }
  ENSURE(openned_pages.size() == pages_full_paths.size());

  PageFooter phdr(lvl, chunk_id);
  phdr.level = lvl;
  phdr.max_chunk_id = chunk_id;
  ENSURE(phdr.max_chunk_id == chunk_id);

  IndexFooter ihdr;

  auto out_file = std::fopen(file_name.c_str(), "ab");
  if (out_file == nullptr) {
    THROW_EXCEPTION("file is null");
  }
  auto index_file_name = PageIndex::index_name_from_page_name(file_name);
  auto out_index_file = std::fopen(index_file_name.c_str(), "ab");
  if (out_index_file == nullptr) {
    THROW_EXCEPTION("can`t open file ", file_name);
  }

  for (auto &kv : links) {
    auto lst = kv.second;
    std::vector<ChunkLink> link_vec(lst.begin(), lst.end());
    std::sort(
        link_vec.begin(), link_vec.end(),
        [](const ChunkLink &left, const ChunkLink &right) { return left.id < right.id; });
    if (logic == nullptr && !PageInner::have_overlap(link_vec)) {
      // don't unpack chunks without overlap. write as is.
      std::unordered_map<std::string, ChunkLinkList> fname2links;
      for (auto link : link_vec) {
        fname2links[link.page_name].push_back(link);
      }

      page_utils::writeChunkToResultPage(fname2links, openned_pages, phdr, ihdr, out_file,
                                         out_index_file);
    } else {
      size_t stored_values_count = size_t(0);

      CursorsList cursors;
      // calculate count of stored values
      for (auto c : link_vec) {
        auto p = openned_pages[c.page_name];
        auto rdr = p->intervalReader(qi, {c});
        for (auto r : rdr) {
          stored_values_count += r.second->count();
          cursors.push_back(r.second);
        }
      }

      MeasArray ma;
      ma.reserve(stored_values_count);
      MArrayPtr_ReaderClb clbk(&ma);
      for (auto r : cursors) {
        r->apply(&clbk);
      }

      ENSURE(ma.size() <= stored_values_count);

      std::sort(ma.begin(), ma.end(), meas_time_compare_less());
      ENSURE(ma.empty() || ma.front().time <= ma.back().time);
      ma.erase(std::unique(ma.begin(), ma.end(),
                           [](auto m1, auto m2) { return m1.time == m2.time; }),
               ma.end());

      if (logic != nullptr) {
        ma = page_utils::applyFilter(ma, logic);
      }

      if (!ma.empty()) {
        std::map<Id, MeasArray> all_values;
        all_values[ma.front().id] = ma;

        auto compressed_results =
            PageInner::compressValues(all_values, phdr, max_chunk_size);

        auto page_size = PageInner::writeToFile(out_file, out_index_file, phdr, ihdr,
                                                *compressed_results, phdr.filesize);
        phdr.filesize = page_size;
      }
    }
  }

  ENSURE(memcmp(&phdr.stat, &ihdr.stat, sizeof(Statistic)) == 0);

  std::fwrite((char *)&phdr, sizeof(PageFooter), 1, out_file);
  std::fclose(out_file);
  ihdr.level = phdr.level;

  std::fwrite(&ihdr, sizeof(IndexFooter), 1, out_index_file);
  std::fclose(out_index_file);
  if (phdr.stat.count == 0) {
    utils::fs::rm(file_name);
    utils::fs::rm(index_file_name);
    return nullptr;
  }
  return open(file_name, phdr);
}

// chunks from memstorage.
Page_Ptr Page::create(const std::string &file_name, uint16_t lvl, uint64_t chunk_id,
                      const std::vector<Chunk *> &a, size_t count) {
  using namespace dariadb::utils::async;

  PageFooter phdr(lvl, chunk_id);

  auto file = std::fopen(file_name.c_str(), "ab");
  if (file == nullptr) {
    throw MAKE_EXCEPTION("WALFile: append error.");
  }

  IndexFooter ihdr;

  auto index_file =
      std::fopen(PageIndex::index_name_from_page_name(file_name).c_str(), "ab");
  if (index_file == nullptr) {
    THROW_EXCEPTION("can`t open file ", file_name);
  }

  uint64_t offset = 0;
  size_t page_size = 0;
  std::vector<IndexReccord> ireccords;
  ireccords.resize(count);
  size_t pos = 0;

  for (size_t i = 0; i < count; ++i) {
    ChunkHeader *chunk_header = a[i]->header;
    auto chunk_buffer_ptr = a[i]->_buffer_t;
#ifdef DEBUG
    {
      auto ch = Chunk::open(chunk_header, chunk_buffer_ptr);
      auto rdr = ch->getReader();
      size_t readed = 0;
      while (!rdr->is_end()) {
        rdr->readNext();
        readed++;
      }
      ENSURE(readed == (ch->header->stat.count));
    }
#endif //  DEBUG
    phdr.max_chunk_id++;
    phdr.stat.update(chunk_header->stat);
    ihdr.stat.update(chunk_header->stat);

    chunk_header->id = phdr.max_chunk_id;

    phdr.addeded_chunks++;
    chunk_header->offset_in_page = offset;

    auto skip_count = Chunk::compact(chunk_header);
    // update checksum;
    Chunk::updateChecksum(*chunk_header, chunk_buffer_ptr + skip_count);

#ifdef DEBUG
    {
      auto ch = Chunk::open(chunk_header, chunk_buffer_ptr + skip_count);
      ENSURE(ch->checkChecksum());
      auto rdr = ch->getReader();
      size_t readed = 0;
      while (!rdr->is_end()) {
        rdr->readNext();
        readed++;
      }
      ENSURE(readed == (ch->header->stat.count));
      ch->close();
    }
#endif //  DEBUG

    std::fwrite(chunk_header, sizeof(ChunkHeader), 1, file);
    std::fwrite(chunk_buffer_ptr + skip_count, sizeof(uint8_t), chunk_header->size, file);

    offset += sizeof(ChunkHeader) + chunk_header->size;

    auto index_reccord = PageInner::init_chunk_index_rec(*chunk_header, &ihdr);
    ireccords[pos] = index_reccord;
    pos++;
  }

  ENSURE(memcmp(&phdr.stat, &ihdr.stat, sizeof(Statistic)) == 0);
  page_size = offset;
  phdr.filesize = page_size;
  std::fwrite(&(phdr), sizeof(PageFooter), 1, file);
  std::fclose(file);

  std::fwrite(ireccords.data(), sizeof(IndexReccord), ireccords.size(), index_file);
  ihdr.level = phdr.level;
  std::fwrite(&ihdr, sizeof(IndexFooter), 1, index_file);
  std::fclose(index_file);

  return Page::open(file_name, phdr);
}

Page_Ptr Page::open(const std::string &file_name) {
  auto phdr = Page::readFooter(file_name);
  auto res = new Page(phdr, file_name);

  res->filename = file_name;
  res->_index = PageIndex::open(PageIndex::index_name_from_page_name(file_name));

  res->footer = phdr;
  return Page_Ptr{res};
}

Page_Ptr Page::open(const std::string &file_name, const PageFooter &phdr) {
  auto res = new Page(phdr, file_name);
  res->_index = PageIndex::open(PageIndex::index_name_from_page_name(file_name));
  return Page_Ptr(res);
}

void Page::restoreIndexFile(const std::string &file_name) {
  logger_info("engine: page - restore index file ", file_name);
  auto phdr = Page::readFooter(file_name);
  auto res = new Page(phdr, file_name);

  res->footer = phdr;
  res->update_index_recs(phdr);
  res->_index = PageIndex::open(PageIndex::index_name_from_page_name(file_name));

  ENSURE(memcmp(&phdr.stat, &res->_index->iheader.stat, sizeof(Statistic)) == 0);
  delete res;
}

PageFooter Page::readFooter(std::string file_name) {
  std::ifstream istream;
  istream.open(file_name, std::fstream::in | std::fstream::binary);
  if (!istream.is_open()) {
    THROW_EXCEPTION("can't open file. filename=", file_name);
  }
  istream.seekg(-(int)sizeof(PageFooter), istream.end);
  PageFooter result(0, 0);
  memset(&result, 0, sizeof(PageFooter));
  istream.read((char *)&result, sizeof(PageFooter));
  istream.close();
  if (!result.check()) {
    THROW_EXCEPTION("PageFooter magic number check.")
  }
  return result;
}

IndexFooter Page::readIndexFooter(std::string ifile) {
  return PageIndex::readIndexFooter(ifile);
}

ChunkLinkList Page::linksByIterval(const QueryInterval &qi) {
  return _index->get_chunks_links(qi.ids, qi.from, qi.to, qi.flag);
}

bool Page::checksum() {
  using dariadb::timeutil::to_string;
  logger_info("engine: checksum page ", this->filename);

  auto page_io = std::fopen(filename.c_str(), "rb");
  if (page_io == nullptr) {
    THROW_EXCEPTION("can`t open file ", this->filename);
  }
  bool result = true;
  auto indexReccords = _index->readReccords();
  for (auto it : indexReccords) {
    Chunk_Ptr c = readChunkByOffset(page_io, it.offset);
    if (!c->checkChecksum()) {
      result = false;
      break;
    }
  }

  std::fclose(page_io);
  return result;
}

void Page::update_index_recs(const PageFooter &phdr) {
  auto index_file =
      std::fopen(PageIndex::index_name_from_page_name(filename).c_str(), "ab");
  if (index_file == nullptr) {
    THROW_EXCEPTION("can`t open file ", this->filename);
  }
  auto page_io = std::fopen(filename.c_str(), "rb");
  if (page_io == nullptr) {
    THROW_EXCEPTION("can`t open file ", this->filename);
  }

  IndexFooter ihdr;

  for (size_t i = 0; i < phdr.addeded_chunks; ++i) {
    ChunkHeader info;
    auto readed = std::fread(&info, sizeof(ChunkHeader), 1, page_io);
    if (readed < size_t(1)) {
      THROW_EXCEPTION("engine: page read error - ", this->filename);
    }
    auto index_reccord = PageInner::init_chunk_index_rec(info, &ihdr);
    ENSURE(index_reccord.offset == info.offset_in_page);
    std::fwrite(&index_reccord, sizeof(IndexReccord), 1, index_file);

    std::fseek(page_io, info.size, SEEK_CUR);
  }
  ihdr.stat = phdr.stat;
  std::fwrite(&ihdr, sizeof(IndexFooter), 1, index_file);
  std::fclose(index_file);
  std::fclose(page_io);
}

bool Page::minMaxTime(dariadb::Id id, dariadb::Time *minTime, dariadb::Time *maxTime) {
  QueryInterval qi{dariadb::IdArray{id}, 0, this->footer.stat.minTime,
                   this->footer.stat.maxTime};
  auto all_chunks = this->linksByIterval(qi);

  bool result = false;
  if (!all_chunks.empty()) {
    result = true;
  }
  *minTime = dariadb::MAX_TIME;
  *maxTime = dariadb::MIN_TIME;
  auto indexReccords = _index->readReccords();
  for (auto &link : all_chunks) {
    auto _index_it = indexReccords[link.index_rec_number];
    *minTime = std::min(*minTime, _index_it.stat.minTime);
    *maxTime = std::max(*maxTime, _index_it.stat.maxTime);
  }
  return result;
}

Chunk_Ptr Page::readChunkByOffset(FILE *page_io, int offset) {
  std::fseek(page_io, offset, SEEK_SET);
  ChunkHeader *cheader = new ChunkHeader;
  auto readed = std::fread(cheader, sizeof(ChunkHeader), 1, page_io);
  if (readed < size_t(1)) {
    delete cheader;
    THROW_EXCEPTION("engine: page read error");
  }
  uint8_t *buffer = new uint8_t[cheader->size];
  memset(buffer, 0, cheader->size);
  readed = std::fread(buffer, cheader->size, 1, page_io);
  if (readed < size_t(1)) {
    delete cheader;
    delete[] buffer;
    THROW_EXCEPTION("engine: page read error");
  }
  Chunk_Ptr ptr = nullptr;
  ptr = Chunk::open(cheader, buffer);
  ptr->is_owner = true;
#ifdef DOUBLE_CHECKS
  if (!ptr->checkChecksum()) {
    logger_fatal("engine: bad checksum of chunk #", ptr->header->id,
                 " for measurement id:", ptr->header->meas_id);
    return nullptr;
  }
#endif
  return ptr;
}

dariadb::Id2Meas Page::valuesBeforeTimePoint(const QueryTimePoint &q) {
  dariadb::Id2Meas result;
  dariadb::IdSet to_read{q.ids.begin(), q.ids.end()};
  auto callback = [&result, &to_read, &q](const Chunk_Ptr &c) {
    auto reader = c->getReader();
    auto m = reader->read_time_point(q);

    auto f_res = result.find(m.id);
    if (f_res == result.end()) {
      to_read.erase(m.id);
      result[m.id] = m;
    } else {
      if (m.time > f_res->first) {
        result[m.id] = m;
      }
    }
    if (to_read.empty()) {
      return true;
    } else {
      return false;
    }
  };
  auto raw_links =
      _index->get_chunks_links(q.ids, _index->iheader.stat.minTime, q.time_point, q.flag);
  if (raw_links.empty()) {
    return result;
  }
  apply_to_chunks(raw_links, callback);
  return result;
}
Id2Cursor Page::intervalReader(const QueryInterval &query) {
  auto links = linksByIterval(query);
  return intervalReader(query, links);
}

Id2Cursor Page::intervalReader(const QueryInterval &query, const ChunkLinkList &links) {

  Id2CursorsList sub_result;
  auto callback = [&sub_result](const Chunk_Ptr &c) {
    auto rdr = c->getReader();
    sub_result[c->header->meas_id].push_back(rdr);
    return false;
  };

  this->apply_to_chunks(links, callback);

  Id2Cursor result = CursorWrapperFactory::colapseCursors(sub_result);
  return result;
}

Statistic Page::stat(const Id id, Time from, Time to) {
  auto links = _index->get_chunks_links({id}, from, to, Flag(0));
  Statistic result;

  auto _ch_links_iterator = links.cbegin();
  if (_ch_links_iterator == links.cend()) {
    return result;
  }
  auto page_io = std::fopen(filename.c_str(), "rb");
  if (page_io == nullptr) {
    THROW_EXCEPTION("can`t open file ", this->filename);
  }
  auto indexReccords = _index->readReccords();
  for (; _ch_links_iterator != links.cend(); ++_ch_links_iterator) {
    if (_ch_links_iterator->meas_id != id) {
      continue;
    }
    auto _index_it = indexReccords[_ch_links_iterator->index_rec_number];
    if (utils::inInterval(from, to, _index_it.stat.minTime) &&
        utils::inInterval(from, to, _index_it.stat.maxTime)) {
      result.update(_index_it.stat);
    } else {
      Chunk_Ptr c = readChunkByOffset(page_io, _index_it.offset);
      if (c == nullptr) {
        continue;
      }
      auto sub_result = c->stat(from, to);
      result.update(sub_result);
    }
  }
  fclose(page_io);
  return result;
}

// callback - return true for break iteration.
void Page::apply_to_chunks(const ChunkLinkList &links,
                           std::function<bool(const Chunk_Ptr &)> callback) {
  auto _ch_links_iterator = links.cbegin();
  if (_ch_links_iterator == links.cend()) {
    return;
  }
  auto page_io = std::fopen(filename.c_str(), "rb");
  if (page_io == nullptr) {
    THROW_EXCEPTION("can`t open file ", this->filename);
  }
  auto indexReccords = _index->readReccords();
  for (; _ch_links_iterator != links.cend(); ++_ch_links_iterator) {
    auto _index_it = indexReccords[_ch_links_iterator->index_rec_number];
    Chunk_Ptr c = readChunkByOffset(page_io, _index_it.offset);
    if (c == nullptr) {
      continue;
    }
    if (callback(c)) {
      break;
    }
  }
  fclose(page_io);
}

void Page::appendChunks(const std::vector<Chunk *> &) {
  NOT_IMPLEMENTED;
}

Id2MinMax_Ptr Page::loadMinMax() {
  Id2MinMax_Ptr result = std::make_shared<Id2MinMax>();
  auto page_io = std::fopen(filename.c_str(), "rb");
  if (page_io == nullptr) {
    THROW_EXCEPTION("can`t open file ", this->filename);
  }
  auto indexReccords = _index->readReccords();
  for (uint32_t i = 0; i < footer.addeded_chunks; ++i) {
    auto _index_it = indexReccords[i];
    Chunk_Ptr search_res = readChunkByOffset(page_io, _index_it.offset);
    if (search_res == nullptr) {
      continue;
    }
    auto info = search_res->header;
    auto fres = result->find_bucket(info->meas_id);
    fres.v->second.updateMax(info->first());
    fres.v->second.updateMin(info->last());
  }
  std::fclose(page_io);
  return result;
}
