#include <libdariadb/storage/index.h>
#include <libdariadb/storage/bloom_filter.h>
#include <algorithm>
#include <cassert>
#include <cstring>
#include <fstream>

using namespace dariadb::storage;
using dariadb::utils::inInterval;

inline bool check_index_rec(IndexReccord &it, dariadb::Time from, dariadb::Time to) {
  return inInterval(from, to, it.minTime) || inInterval(from, to, it.maxTime) ||
         inInterval(it.minTime, it.maxTime, from) ||
         inInterval(it.minTime, it.maxTime, to);
}

inline bool check_blooms(const IndexReccord &_index_it, dariadb::Id id,
                         dariadb::Flag flag) {
  auto id_check_result = false;
  id_check_result=_index_it.meas_id==id;
  auto flag_bloom_result = false;
  if (flag == dariadb::Flag(0) ||
      dariadb::storage::bloom_check(_index_it.flag_bloom, flag)) {
    flag_bloom_result = true;
  }
  return id_check_result && flag_bloom_result;
}

PageIndex::~PageIndex() {
  iheader->is_closed = true;
  index = nullptr;
  index_mmap->close();
}

PageIndex_ptr PageIndex::create(const std::string &filename, uint64_t size) {
  PageIndex_ptr res = std::make_shared<PageIndex>();
  auto immap = utils::fs::MappedFile::touch(filename, size);
  auto iregion = immap->data();
  std::fill(iregion, iregion + size, 0);
  res->index_mmap = immap;
  res->iregion = iregion;

  res->iheader = reinterpret_cast<IndexHeader *>(iregion);
  res->index = reinterpret_cast<IndexReccord *>(iregion + sizeof(IndexHeader));

  res->iheader->maxTime = dariadb::MIN_TIME;
  res->iheader->minTime = dariadb::MAX_TIME;
  res->iheader->is_sorted = false;
  res->iheader->id_bloom = storage::bloom_empty<dariadb::Id>();
  res->iheader->is_closed = false;
  res->index_mmap->flush();
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
  res->index = reinterpret_cast<IndexReccord *>(iregion + sizeof(IndexHeader));
  res->iheader->is_closed = false;
  res->index_mmap->flush();
  return res;
}

ChunkLinkList PageIndex::get_chunks_links(const dariadb::IdArray &ids, dariadb::Time from,
                                          dariadb::Time to, dariadb::Flag flag) {
  ChunkLinkList result;
  for (uint32_t pos = 0; pos < this->iheader->count; ++pos) {

    auto _index_it = this->index[pos];
    if (!_index_it.is_init) {
      continue;
    }
    if (check_index_rec(_index_it, from, to)) {
      bool bloom_result = false;
	  if (ids.size() == size_t(0)) {
		  bloom_result = true;
	  }
	  else {
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
		sub_result.minTime = _index_it.minTime;
        sub_result.maxTime = _index_it.maxTime;
        sub_result.meas_id = _index_it.meas_id;
        result.push_back(sub_result);
      }
    }
  }

  return result;
}

IndexHeader PageIndex::readIndexHeader(std::string ifile) {
  std::ifstream istream;
  istream.open(ifile, std::fstream::in | std::fstream::binary);
  if (!istream.is_open()) {
    THROW_EXCEPTION("can't open file. filename=" , ifile);
  }
  IndexHeader result;
  memset(&result, 0, sizeof(IndexHeader));
  istream.read((char *)&result, sizeof(IndexHeader));
  istream.close();
  return result;
}
