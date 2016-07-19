#include "index.h"
#include "bloom_filter.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <fstream>

#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>

using namespace dariadb::storage;
using dariadb::utils::inInterval;

inline bool check_index_rec(IndexReccord &it, dariadb::Time from, dariadb::Time to) {
  return inInterval(from, to, it.minTime) || inInterval(from, to, it.maxTime) ||
         inInterval(it.minTime, it.maxTime, from) ||
         inInterval(it.minTime, it.maxTime, to);
}

inline bool check_blooms(const IndexReccord &_index_it, dariadb::Id id, dariadb::Flag flag) {
	auto id_bloom_result = false;
	if (dariadb::storage::bloom_check(_index_it.id_bloom, id)) {
		id_bloom_result = true;
	}
	auto flag_bloom_result = false;
	if (flag == dariadb::Flag(0) ||
		dariadb::storage::bloom_check(_index_it.flag_bloom, flag)) {
		flag_bloom_result = true;
	}
	return id_bloom_result && flag_bloom_result;
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
  boost::shared_lock<boost::shared_mutex> lg(_locker);
  ChunkLinkList result;
  for (uint32_t pos = 0; pos < this->iheader->count; ++pos) {

    auto _index_it = this->index[pos];
    if (!_index_it.is_init || !_index_it.commit) {
      continue;
    }
    if (check_index_rec(_index_it, from, to)) {
      bool bloom_result = false;
      for (auto i : ids) {
        bloom_result = check_blooms(_index_it, i, flag);
        if (bloom_result) {
          break;
        }
      }
      if (bloom_result) {
        ChunkLink sub_result;
        sub_result.id = _index_it.chunk_id;
        sub_result.pos = pos;
        sub_result.maxTime = _index_it.maxTime;
        sub_result.id_bloom = _index_it.id_bloom;
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

void PageIndex::update_index_info(IndexReccord *cur_index, const Chunk_Ptr &ptr,
                                  const dariadb::Meas &m, uint32_t pos) {
  assert(cur_index->chunk_id == ptr->header->id);
  assert(ptr->header->pos_in_page == pos);
  
  iheader->id_bloom = storage::bloom_add(iheader->id_bloom, m.id);
  iheader->minTime = std::min(iheader->minTime, ptr->header->minTime);
  iheader->maxTime = std::max(iheader->maxTime, ptr->header->maxTime);
  iheader->transaction = std::max(iheader->transaction, ptr->header->transaction);

  cur_index->minTime = std::min(cur_index->minTime, m.time);
  cur_index->maxTime = std::max(cur_index->maxTime, m.time);
  cur_index->flag_bloom = ptr->header->flag_bloom;
  cur_index->id_bloom = ptr->header->id_bloom;
}
