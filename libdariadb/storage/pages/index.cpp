#ifdef MSVC
#define _CRT_SECURE_NO_WARNINGS // for fopen
#endif
#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/pages/index.h>
#include <algorithm>

#include <cstring>
#include <fstream>

using namespace dariadb;
using namespace dariadb::storage;
using dariadb::utils::inInterval;

inline bool check_index_rec(IndexReccord &it, dariadb::Time from, dariadb::Time to) {
  return inInterval(from, to, it.stat.minTime) || inInterval(from, to, it.stat.maxTime) ||
         inInterval(it.stat.minTime, it.stat.maxTime, from) ||
         inInterval(it.stat.minTime, it.stat.maxTime, to);
}

inline bool check_blooms(const IndexReccord &_index_it, dariadb::Id id,
                         dariadb::Flag flag) {
  auto id_check_result = false;
  id_check_result = _index_it.target_id == id;
  auto flag_bloom_result = false;
  if (flag == dariadb::Flag(0) ||
      dariadb::storage::bloom_check(_index_it.stat.flag_bloom, flag)) {
    flag_bloom_result = true;
  }
  return id_check_result && flag_bloom_result;
}

PageIndex::~PageIndex() {}

PageIndex_ptr PageIndex::open(const std::string &_filename) {
  PageIndex_ptr res = std::make_shared<PageIndex>();
  res->filename = _filename;
  res->iheader = readIndexFooter(_filename);
  return res;
}

ChunkLinkList PageIndex::get_chunks_links(const dariadb::IdArray &ids, dariadb::Time from,
                                          dariadb::Time to, dariadb::Flag flag) {
  ChunkLinkList result;
  IndexReccord *records = new IndexReccord[this->iheader.recs_count];
  auto index_file = std::fopen(filename.c_str(), "rb");
  if (index_file == nullptr) {
    delete[] records;
    THROW_EXCEPTION("can`t open file ", this->filename);
  }

  auto readed = std::fread(records, sizeof(IndexReccord), iheader.recs_count, index_file);
  if (readed < iheader.recs_count) {
    delete[] records;
    THROW_EXCEPTION("engine: index read error - ", this->filename);
  }
  std::fclose(index_file);
  for (uint32_t pos = 0; pos < this->iheader.recs_count; ++pos) {

    auto _index_it = records[pos];
    if (check_index_rec(_index_it, from, to)) {
      bool bloom_result = false;
      if (ids.size() == size_t(0)) {
        bloom_result = true;
      } else {
        for (auto i : ids) {
          bloom_result = check_blooms(_index_it, i, flag);
          if (bloom_result) {
            break;
          }
        }
      }
      if (bloom_result) {
        ChunkLink sub_result;
        sub_result.id = _index_it.chunk_id;
        sub_result.index_rec_number = pos;
        sub_result.minTime = _index_it.stat.minTime;
        sub_result.maxTime = _index_it.stat.maxTime;
        sub_result.meas_id = _index_it.target_id;
        result.push_back(sub_result);
      }
    }
  }
  delete[] records;

  return result;
}

std::vector<IndexReccord> PageIndex::readReccords() {
  std::vector<IndexReccord> records;
  records.resize(iheader.recs_count);

  auto index_file = std::fopen(filename.c_str(), "rb");
  if (index_file == nullptr) {
    THROW_EXCEPTION("can`t open file ", this->filename);
  }
  auto readed =
      std::fread(records.data(), sizeof(IndexReccord), iheader.recs_count, index_file);
  if (readed < iheader.recs_count) {
    THROW_EXCEPTION("engine: index read error - ", this->filename);
  }
  std::fclose(index_file);
  return records;
}

IndexFooter PageIndex::readIndexFooter(std::string ifile) {
  std::ifstream istream;
  istream.open(ifile, std::fstream::in | std::fstream::binary);
  if (!istream.is_open()) {
    THROW_EXCEPTION("can't open file. filename=", ifile);
  }
  istream.seekg(-(int)sizeof(IndexFooter), istream.end);
  IndexFooter result;
  memset(&result, 0, sizeof(IndexFooter));
  istream.read((char *)&result, sizeof(IndexFooter));
  istream.close();
  if (!result.check()) {
    THROW_EXCEPTION("IndexFooter magic number check.");
  }
  return result;
}
