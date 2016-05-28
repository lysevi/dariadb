#include "page_manager.h"
#include "../utils/asyncworker.h"
#include "../utils/fs.h"
#include "../utils/locker.h"
#include "../utils/lru.h"
#include "../utils/utils.h"
#include "bloom_filter.h"
#include "manifest.h"
#include "page.h"

#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <memory>
#include <queue>
#include <thread>

const std::string MANIFEST_FILE_NAME = "Manifest";

using namespace dariadb::storage;
dariadb::storage::PageManager *PageManager::_instance = nullptr;
// PM
class PageManager::Private /*:public dariadb::utils::AsyncWorker<Chunk_Ptr>*/ {
public:
  Private(const PageManager::Params &param)
      : _cur_page(nullptr), _param(param),
        _manifest(utils::fs::append_path(param.path, MANIFEST_FILE_NAME)),
        _openned_pages(param.openned_page_chache_size) {
    update_id = false;
    last_id = 0;

    _cur_page = open_last_openned();
    /*this->start_async();*/
  }

  Page_Ptr open_last_openned() {
    if (!utils::fs::path_exists(_param.path)) {
      return nullptr;
    }
    auto pages = _manifest.page_list();

    for (auto n : pages) {
      auto file_name = utils::fs::append_path(_param.path, n);
      auto hdr = Page::readHeader(file_name);
      if (!hdr.is_full) {
        auto res = Page_Ptr{Page::open(file_name)};
        update_id = false;
        return res;
      } else {
        last_id = std::max(last_id, hdr.max_chunk_id);
        update_id = true;
      }
    }
    return nullptr;
  }

  ~Private() {
    /* this->stop_async();*/

    if (_cur_page != nullptr) {
      _cur_page = nullptr;
    }
    _openned_pages.clear();
  }

  uint64_t calc_page_size() const {
    auto sz_info = _param.chunk_per_storage * sizeof(ChunkIndexInfo);
    auto sz_buffers = _param.chunk_per_storage * _param.chunk_size;
    return sizeof(PageHeader) + sz_buffers + sz_info;
  }

  Page_Ptr create_page() {
    if (!dariadb::utils::fs::path_exists(_param.path)) {
      dariadb::utils::fs::mkdir(_param.path);
    }

    Page *res = nullptr;

    std::string page_name = utils::fs::random_file_name(".page");
    std::string file_name = dariadb::utils::fs::append_path(_param.path, page_name);
    auto sz = calc_page_size();
    res = Page::create(file_name, sz, _param.chunk_per_storage, _param.chunk_size);
    _manifest.page_append(page_name);
    if (update_id) {
      res->header->max_chunk_id = last_id;
    }

    return Page_Ptr{res};
  }
  // PM
  void flush() { /*this->flush_async();*/
  }
  // void call_async(const Chunk_Ptr &ch) override { /*write_to_page(ch);*/ }

  Page_Ptr get_cur_page() {
    if (_cur_page == nullptr) {
      _cur_page = create_page();
    }
    return _cur_page;
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult, dariadb::Time *maxResult) {
    boost::shared_lock<boost::shared_mutex> lg(_locker);

    auto pages = pages_by_filter(
        [id](const IndexHeader &ih) { return (storage::bloom_check(ih.id_bloom, id)); });
    *minResult = dariadb::MAX_TIME;
    *maxResult = dariadb::MIN_TIME;
    auto res = false;
    for (auto pname : pages) {
      Page_Ptr pg = open_page_to_read(pname);
      dariadb::Time local_min, local_max;
      if (pg->minMaxTime(id, &local_min, &local_max)) {
        *minResult = std::min(local_min, *minResult);
        *maxResult = std::max(local_max, *maxResult);
        res = true;
      }
    }
    return res;
  }

  Page_Ptr open_page_to_read(const std::string &pname) {
    Page_Ptr pg = nullptr;
    if (_cur_page != nullptr && pname == _cur_page->filename) {
      pg = _cur_page;
    } else {
      if (_openned_pages.find(pname, &pg)) {
        return pg;
      } else {
        pg = Page_Ptr{Page::open(pname, true)};
        Page_Ptr dropped;
        _openned_pages.put(pname, pg, &dropped);
        dropped = nullptr;
      }
    }
    return pg;
  }

  class PageManagerCursor : public Cursor {
  public:
    bool is_end() const override { return _chunk_iterator == chunks.end(); }
    void readNext(Callback *cbk) override {
      if (!is_end()) {
        cbk->call(*_chunk_iterator);
        ++_chunk_iterator;
      }
    }
    void reset_pos() override { _chunk_iterator = chunks.begin(); }

    ChunksList chunks;
    ChunksList::iterator _chunk_iterator;
  };

  class AddCursorClbk : public Cursor::Callback {
  public:
    void call(dariadb::storage::Chunk_Ptr &ptr) {
      if (ptr != nullptr) {
        (*out).push_back(ptr);
      }
    }

    dariadb::storage::ChunksList *out;
  };

  ChunkLinkList chunksByIterval(const QueryInterval &query) {
    boost::shared_lock<boost::shared_mutex> lg(_locker);

    auto pred = [query](const IndexHeader &hdr) {
      auto interval_check((hdr.minTime >= query.from && hdr.maxTime <= query.to) ||
                          (utils::inInterval(query.from, query.to, hdr.minTime)) ||
                          (utils::inInterval(query.from, query.to, hdr.maxTime)) ||
                          (utils::inInterval(hdr.minTime, hdr.maxTime, query.from)) ||
                          (utils::inInterval(hdr.minTime, hdr.maxTime, query.to)));
      if (interval_check) {
        for (auto id : query.ids) {
          if (storage::bloom_check(hdr.id_bloom, id)) {
            return true;
          }
        }
      }
      return false;
    };
    auto page_list = pages_by_filter(std::function<bool(const IndexHeader &)>(pred));

    ChunkLinkList result;
    for (auto pname : page_list) {
      /* auto pg = open_page_to_read(pname);

       auto sub_result = pg->chunksByIterval(query);*/
      auto pi = PageIndex::open(PageIndex::index_name_from_page_name(pname), true);
      auto sub_result = pi->get_chunks_links(query.ids, query.from, query.to, query.flag);
      for (auto s : sub_result) {
        s.page_name = pname;
        result.push_back(s);
      }
    }

    /*std::vector<ChunkLink> forSort{ result.begin(),result.end() };
    std::sort(forSort.begin(), forSort.end(),
            [](const dariadb::storage::ChunkLink&l, const dariadb::storage::ChunkLink&r)
    {
            return l.maxTime < r.maxTime;
    }
    );*/

    return result; // ChunkLinkList(forSort.begin(), forSort.end());
  }

  Cursor_ptr readLinks(const ChunkLinkList &links) {
    boost::shared_lock<boost::shared_mutex> lg(_locker);

    PageManagerCursor *raw_cursor = new PageManagerCursor;
    dariadb::storage::ChunksList chunks;

    std::unique_ptr<AddCursorClbk> clbk{new AddCursorClbk};
    clbk->out = &chunks;

    ChunkLinkList to_read;

    for (auto l : links) {
      if (to_read.empty()) {
        to_read.push_back(l);
      } else {
        if (l.page_name == to_read.front().page_name) {
          to_read.push_back(l);
        } else {
          auto pname = to_read.front().page_name;
          Page_Ptr pg = open_page_to_read(pname);
          pg->readLinks(to_read)->readAll(clbk.get());
          to_read.clear();
          to_read.push_back(l);
        }
      }
    }
    if (!to_read.empty()) {
      auto pname = to_read.front().page_name;
      auto pg = open_page_to_read(pname);
      pg->readLinks(to_read)->readAll(clbk.get());

      to_read.clear();
    }

    /*for (auto kv : page2links) {
            auto p = Page::open(kv.first);
            p->readLinks(kv.second)->readAll(clbk.get());
            delete p;
    }*/

    for (auto &ch : chunks) {
      raw_cursor->chunks.push_back(ch);
    }
    raw_cursor->reset_pos();
    return Cursor_ptr{raw_cursor};
  }
  std::list<std::string> pages_by_filter(std::function<bool(const IndexHeader &)> pred) {
    std::list<std::string> result;
    auto names = _manifest.page_list();
    for (auto n : names) {
      auto index_file_name = utils::fs::append_path(_param.path, n + "i");
      auto hdr = Page::readIndexHeader(index_file_name);
      if (pred(hdr)) {
        auto page_file_name = utils::fs::append_path(_param.path, n);
        result.push_back(page_file_name);
      }
    }
    return result;
  }

  Meas::Id2Meas valuesBeforeTimePoint(const QueryTimePoint &query) {
    boost::shared_lock<boost::shared_mutex> lg(_locker);

    Meas::Id2Meas result;

    auto pred = [query](const IndexHeader &hdr) {
      auto in_check = utils::inInterval(hdr.minTime, hdr.maxTime, query.time_point) ||
                      (hdr.maxTime < query.time_point);
      if (in_check) {
        for (auto id : query.ids) {
          if (storage::bloom_check(hdr.id_bloom, id)) {
            return true;
          }
        }
      }
      return false;
    };
    auto page_list = pages_by_filter(std::function<bool(IndexHeader)>(pred));

    for (auto it = page_list.rbegin(); it != page_list.rend(); ++it) {
      auto pname = *it;
      auto pg = open_page_to_read(pname);

      auto subres = pg->valuesBeforeTimePoint(query);
      for (auto kv : subres) {
        result.insert(kv);
      }
      if (subres.size() == query.ids.size()) {
        break;
      }
    }

    return result;
  }

  size_t chunks_in_cur_page() const {
    if (_cur_page == nullptr) {
      return 0;
    }
    return _cur_page->header->addeded_chunks;
  }
  // PM
  size_t in_queue_size() const { return 0; /*return this->async_queue_size();*/ }

  dariadb::Time minTime() {
    boost::shared_lock<boost::shared_mutex> lg(_locker);

    auto pred = [](const IndexHeader &) { return true; };

    auto page_list = pages_by_filter(std::function<bool(IndexHeader)>(pred));

    dariadb::Time res = dariadb::MAX_TIME;
    for (auto pname : page_list) {
      auto ih = Page::readIndexHeader(pname + "i");
      res = std::min(ih.minTime, res);
    }

    return res;
  }
  dariadb::Time maxTime() {
    boost::shared_lock<boost::shared_mutex> lg(_locker);

    auto pred = [](const IndexHeader &) { return true; };

    auto page_list = pages_by_filter(std::function<bool(IndexHeader)>(pred));

    dariadb::Time res = dariadb::MAX_TIME;
    for (auto pname : page_list) {
      auto ih = Page::readIndexHeader(pname + "i");
      res = std::max(ih.maxTime, res);
    }

    return res;
  }

  append_result append(const Meas &value) {
    boost::upgrade_lock<boost::shared_mutex> lg(_locker);

    while (true) {
      auto cur_page = this->get_cur_page();
      if (update_id) {
        update_id = false;
      }
      auto res = cur_page->append(value);
      if (res.writed != 1) {
        last_id = _cur_page->header->max_chunk_id;
        update_id = true;
        _cur_page = nullptr;
      } else {
        return res;
      }
    }
  }

protected:
  Page_Ptr _cur_page;
  PageManager::Params _param;
  Manifest _manifest;
  boost::shared_mutex _locker;

  uint64_t last_id;
  bool update_id;
  utils::LRU<std::string, Page_Ptr> _openned_pages;
};

PageManager::PageManager(const PageManager::Params &param)
    : impl(new PageManager::Private{param}) {}

PageManager::~PageManager() {}

void PageManager::start(const PageManager::Params &param) {
  if (PageManager::_instance == nullptr) {
    PageManager::_instance = new PageManager(param);
  }
}

void PageManager::stop() {
  if (_instance != nullptr) {
    delete PageManager::_instance;
    _instance = nullptr;
  }
}

void PageManager::flush() {
  this->impl->flush();
}

PageManager *PageManager::instance() {
  return _instance;
}
// PM
// bool PageManager::append(const Chunk_Ptr &c) {
//  return impl->append(c);
//}
//
// bool PageManager::append(const ChunksList &c) {
//  return impl->append(c);
//}

// Cursor_ptr PageManager::get_chunks(const dariadb::IdArray&ids, dariadb::Time
// from, dariadb::Time to, dariadb::Flag flag) {
//    return impl->get_chunks(ids, from, to, flag);
//}
// ChunkContainer
bool PageManager::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                             dariadb::Time *maxResult) {
  return impl->minMaxTime(id, minResult, maxResult);
}

dariadb::storage::ChunkLinkList PageManager::chunksByIterval(const QueryInterval &query) {
  return impl->chunksByIterval(query);
}

dariadb::Meas::Id2Meas PageManager::valuesBeforeTimePoint(const QueryTimePoint &q) {
  return impl->valuesBeforeTimePoint(q);
}

dariadb::storage::Cursor_ptr PageManager::readLinks(const ChunkLinkList &links) {
  return impl->readLinks(links);
}

size_t PageManager::in_queue_size() const {
  return impl->in_queue_size();
}

dariadb::Time PageManager::minTime() {
  return impl->minTime();
}

dariadb::Time PageManager::maxTime() {
  return impl->maxTime();
}

dariadb::append_result dariadb::storage::PageManager::append(const Meas &value) {
  return impl->append(value);
}
