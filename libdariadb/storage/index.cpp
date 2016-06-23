#include "index.h"
#include "bloom_filter.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <fstream>

#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>

using namespace dariadb::storage;

class PageLinksCursor {
public:
  PageLinksCursor(PageIndex *page, const dariadb::IdArray &ids, dariadb::Time from,
                  dariadb::Time to, dariadb::Flag flag)
      : link(page), _ids(ids), _from(from), _to(to), _flag(flag) {}

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

      ChunkLink sub_result;
      sub_result.id = _index_it.chunk_id;
      sub_result.pos = current_pos;
      sub_result.maxTime = _index_it.maxTime;
      sub_result.id_bloom = _index_it.id_bloom;
      this->resulted_links.push_back(sub_result);
      break;
    }
    if (read_poses.empty()) {
      _is_end = true;
      return;
    }
  }

  bool check_index_rec(IndexReccord &it) const {
    return ((dariadb::utils::inInterval(_from, _to, it.minTime)) ||
            (dariadb::utils::inInterval(_from, _to, it.maxTime))) ||
           (dariadb::utils::inInterval(it.minTime, it.maxTime, _from) ||
            dariadb::utils::inInterval(it.minTime, it.maxTime, _to));
  }

  void reset_pos() { // start read from begining;
    _is_end = false;
    this->read_poses.clear();

    for (uint32_t pos = 0; pos < this->link->iheader->count; ++pos) {

      auto _index_it = link->index[pos];
      if (!_index_it.is_init) {
        continue;
      }
      if (dariadb::utils::inInterval(_index_it.minTime, _index_it.maxTime, _from) ||
          dariadb::utils::inInterval(_index_it.minTime, _index_it.maxTime, _to) ||
          dariadb::utils::inInterval(_from, _to, _index_it.minTime) ||
          dariadb::utils::inInterval(_from, _to, _index_it.maxTime)) {
        bool bloom_result = false;
        for (auto i : _ids) {
          bloom_result = check_blooms(_index_it, i);
          if (bloom_result) {
            break;
          }
        }
        if (bloom_result) {
          if (check_index_rec(_index_it)) {
            this->read_poses.push_back(pos);
            assert(this->read_poses.size() <= this->link->iheader->count);
          }
        }
      }
    }
    if (read_poses.empty()) {
      _is_end = true;
    }
  }

  bool check_blooms(const IndexReccord &_index_it, dariadb::Id id) const {
    auto id_bloom_result = false;
    if (dariadb::storage::bloom_check(_index_it.id_bloom, id)) {
      id_bloom_result = true;
    }
    auto flag_bloom_result = false;
    if (_flag==dariadb::Flag(0) || dariadb::storage::bloom_check(_index_it.flag_bloom, _flag)) {
      flag_bloom_result = true;
    }
	if (id_bloom_result && !flag_bloom_result) {
		std::cout << 1;
	}
    return id_bloom_result && flag_bloom_result;
  }

  ChunkLinkList resulted_links;

protected:
  PageIndex *link;
  bool _is_end;
  dariadb::IdArray _ids;
  dariadb::Time _from, _to;
  dariadb::Flag _flag;
  std::list<uint32_t> read_poses;
};

PageIndex::~PageIndex() {
  if (!readonly && !iheader->is_sorted) {
    size_t pos = 0; // TODO crash safety
    IndexReccord *new_index = new IndexReccord[iheader->chunk_per_storage];
    memset(new_index, 0, sizeof(IndexReccord) * iheader->chunk_per_storage);

    for (auto it = _itree.begin(); it != _itree.end(); ++it, ++pos) {
      new_index[pos] = index[it->second];
    }
    memcpy(index, new_index, sizeof(IndexReccord) * iheader->chunk_per_storage);
    delete[] new_index;
    iheader->is_sorted = true;
  }
  iheader->is_closed = true;
  _itree.clear();
  index = nullptr;
  index_mmap->close();
}

PageIndex_ptr PageIndex::create(const std::string &filename, uint64_t size,
                                uint32_t chunk_per_storage, uint32_t chunk_size) {
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
  res->iheader->chunk_per_storage = chunk_per_storage;
  res->iheader->chunk_size = chunk_size;
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
  // assert(res->iheader->is_closed);
  res->iheader->is_closed = false;
  res->index_mmap->flush();
  return res;
}

ChunkLinkList PageIndex::get_chunks_links(const dariadb::IdArray &ids, dariadb::Time from,
                                          dariadb::Time to, dariadb::Flag flag) {
  boost::shared_lock<boost::shared_mutex> lg(_locker);

  PageLinksCursor c(this, ids, from, to, flag);
  c.reset_pos();

  while (!c.is_end()) {
    c.readNext();
  }

  return c.resulted_links;
}

void PageIndex::update_index_info(IndexReccord *cur_index, const Chunk_Ptr &ptr,
                                  const dariadb::Meas &m, uint32_t pos) {
  // cur_index->last = ptr->info->last;
  iheader->id_bloom = storage::bloom_add(iheader->id_bloom, m.id);
  iheader->minTime = std::min(iheader->minTime, ptr->header->minTime);
  iheader->maxTime = std::max(iheader->maxTime, ptr->header->maxTime);

  for (auto it = _itree.lower_bound(cur_index->maxTime);
       it != _itree.upper_bound(cur_index->maxTime); ++it) {
    if ((it->first == cur_index->maxTime) && (it->second == pos)) {
      _itree.erase(it);
      break;
    }
  }

  cur_index->minTime = std::min(cur_index->minTime, m.time);
  cur_index->maxTime = std::max(cur_index->maxTime, m.time);
  cur_index->flag_bloom = ptr->header->flag_bloom;
  cur_index->id_bloom = ptr->header->id_bloom;
  auto kv = std::make_pair(cur_index->maxTime, pos);
  _itree.insert(kv);
}
