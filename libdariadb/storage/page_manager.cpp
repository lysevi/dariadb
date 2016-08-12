#include "page_manager.h"
#include "../flags.h"
#include "../utils/fs.h"
#include "../utils/locker.h"
#include "../utils/lru.h"
#include "../utils/metrics.h"
#include "../utils/thread_manager.h"
#include "../utils/utils.h"
#include "../utils/utils.h"
#include "bloom_filter.h"
#include "manifest.h"
#include "options.h"
#include "page.h"

#include <atomic>
#include <condition_variable>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <thread>
#include <unordered_map>

using namespace dariadb::storage;
using namespace dariadb::utils::async;

dariadb::storage::PageManager *PageManager::_instance = nullptr;

using File2PageHeader = std::unordered_map<std::string, IndexHeader>;

class PageManager::Private {
public:
  Private()
      : _cur_page(nullptr),
        _openned_pages(Options::instance()->page_openned_page_cache_size) {

    update_id = false;
    last_id = 0;

    if (utils::fs::path_exists(Options::instance()->path)) {
      auto pages = Manifest::instance()->page_list();

      for (auto n : pages) {
        auto file_name = utils::fs::append_path(Options::instance()->path, n);
        auto hdr = Page::readIndexHeader(PageIndex::index_name_from_page_name(file_name));
        _file2header[n] = hdr;
      }
    }
  }

  ~Private() {
    if (_cur_page != nullptr) {
      _cur_page = nullptr;
    }
    _openned_pages.clear();
  }

  void fsck(bool force_check) {
    if (force_check) {
      logger_info("PageManager: fsck force");
    }
    if (!utils::fs::path_exists(Options::instance()->path)) {
      return;
    }

    auto pages = Manifest::instance()->page_list();

    for (auto n : pages) {
      auto file_name = utils::fs::append_path(Options::instance()->path, n);
      auto hdr = Page::readHeader(file_name);
      if (hdr.removed_chunks == hdr.addeded_chunks) {
        logger_info("page: " << file_name << " is empty.");
        erase_page(file_name);
      } else {
        if (force_check || (!hdr.is_closed && hdr.is_open_to_write)) {
          auto res = Page_Ptr{Page::open(file_name)};
          res->fsck();
          res = nullptr;
        }
      }
    }
  }

  /// file_name - full path.
  void erase_page(const std::string &file_name) {
    auto target_name = utils::fs::extract_filename(file_name);
    logger_info("page: " << file_name << " removing...");
    erase(target_name);
  }

  void erase(const std::string &fname) {
    auto full_file_name = utils::fs::append_path(Options::instance()->path, fname);
    _openned_pages.erase(full_file_name);

    Manifest::instance()->page_rm(fname);
    utils::fs::rm(full_file_name);
    utils::fs::rm(PageIndex::index_name_from_page_name(full_file_name));
    _file2header.erase(fname);
  }
  // PM
  void flush() {}

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult, dariadb::Time *maxResult) {
    TIMECODE_METRICS(ctmd, "minMaxTime", "PageManager::minMaxTime");

    auto pages = pages_by_filter(
        [id](const IndexHeader &ih) { return (storage::bloom_check(ih.id_bloom, id)); });

    using MMRes = std::tuple<bool, dariadb::Time, dariadb::Time>;
    std::vector<MMRes> results{pages.size()};
    std::vector<TaskResult_Ptr> task_res{pages.size()};
    size_t num = 0;

    for (auto pname : pages) {
      AsyncTask at = [pname, &results, num, this, id](const ThreadInfo &ti) {
        TKIND_CHECK(THREAD_COMMON_KINDS::FILE_READ, ti.kind);
        Page_Ptr pg = open_page_to_read(pname);
        dariadb::Time lmin, lmax;
        if (pg->minMaxTime(id, &lmin, &lmax)) {
          results[num] = MMRes(true, lmin, lmax);
        } else {
          results[num] = MMRes(false, lmin, lmax);
        }
      };
      task_res[num] =
          ThreadManager::instance()->post(THREAD_COMMON_KINDS::FILE_READ, AT(at));
      num++;
    }

    for (auto &tw : task_res) {
      tw->wait();
    }

    bool res = false;

    *minResult = dariadb::MAX_TIME;
    *maxResult = dariadb::MIN_TIME;
    for (auto &subRes : results) {
      if (std::get<0>(subRes)) {
        res = true;
        *minResult = std::min(std::get<1>(subRes), *minResult);
        *maxResult = std::max(std::get<2>(subRes), *maxResult);
      }
    }

    return res;
  }

  Page_Ptr open_page_to_read(const std::string &pname) {
    TIMECODE_METRICS(ctmd, "open", "PageManager::open_page_to_read");
    std::lock_guard<std::mutex> lg(_page_open_lock);
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
        if (dropped != nullptr) {
          _file2header.erase(dropped->filename);
        }
        /*if (dropped != nullptr) {
          _openned_pages.set_max_size(_openned_pages.size() + 1);
          Page_Ptr should_be_null;
          if (_openned_pages.put(dropped->filename, dropped, &should_be_null)) {
            throw MAKE_EXCEPTION("LRU cache logic wrong.");
          }
        }*/
      }
    }
    return pg;
  }

  ChunkLinkList chunksByIterval(const QueryInterval &query) {
    TIMECODE_METRICS(ctmd, "read", "PageManager::chunksByIterval");

    auto pred = [query](const IndexHeader &hdr) {
      auto interval_check((hdr.minTime >= query.from && hdr.maxTime <= query.to) ||
                          (utils::inInterval(query.from, query.to, hdr.minTime)) ||
                          (utils::inInterval(query.from, query.to, hdr.maxTime)) ||
                          (utils::inInterval(hdr.minTime, hdr.maxTime, query.from)) ||
                          (utils::inInterval(hdr.minTime, hdr.maxTime, query.to)));
      if (interval_check) {
        for (auto id : query.ids) {
          if (storage::bloom_check(hdr.id_bloom, id) &&
              (query.flag == Flag(0) ||
               storage::bloom_check(hdr.flag_bloom, query.flag))) {
            return true;
          }
        }
      }
      return false;
    };
    auto page_list = pages_by_filter(std::function<bool(const IndexHeader &)>(pred));

    ChunkLinkList result;
    for (auto pname : page_list) {
      auto pi = PageIndex::open(PageIndex::index_name_from_page_name(pname), true);
      auto sub_result = pi->get_chunks_links(query.ids, query.from, query.to, query.flag);
      for (auto s : sub_result) {
        s.page_name = pname;
        result.push_back(s);
      }
    }

    return result;
  }

  void readLinks(const QueryInterval &query, const ChunkLinkList &links,
                 IReaderClb *clb) {
    TIMECODE_METRICS(ctmd, "read", "PageManager::readLinks");

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
          pg->readLinks(query, to_read, clb);
          to_read.clear();
          to_read.push_back(l);
        }
      }
    }
    if (!to_read.empty()) {
      auto pname = to_read.front().page_name;
      auto pg = open_page_to_read(pname);
      pg->readLinks(query, to_read, clb);
      to_read.clear();
    }
  }

  std::list<std::string> pages_by_filter(std::function<bool(const IndexHeader &)> pred) {
    TIMECODE_METRICS(ctmd, "read", "PageManager::pages_by_filter");
    std::list<std::string> result;

    for (auto f2h : _file2header) {
      auto hdr = f2h.second;
      if (pred(hdr)) {
        auto page_file_name =
            utils::fs::append_path(Options::instance()->path, f2h.first);
        result.push_back(page_file_name);
      }
    }
    return result;
  }

  Meas::Id2Meas valuesBeforeTimePoint(const QueryTimePoint &query) {
    TIMECODE_METRICS(ctmd, "readInTimePoint", "PageManager::valuesBeforeTimePoint");

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

    Meas::Id2Meas result;

    for (auto id : query.ids) {
      result[id].flag = Flags::_NO_DATA;
      result[id].time = query.time_point;
    }

    auto page_list = pages_by_filter(std::function<bool(IndexHeader)>(pred));

    for (auto it = page_list.rbegin(); it != page_list.rend(); ++it) {
      auto pname = *it;
      auto pg = open_page_to_read(pname);

      auto subres = pg->valuesBeforeTimePoint(query);
      for (auto kv : subres) {
        result[kv.first] = kv.second;
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
  size_t files_count() const { return Manifest::instance()->page_list().size(); }

  dariadb::Time minTime() {

    auto pred = [](const IndexHeader &) { return true; };

    auto page_list = pages_by_filter(std::function<bool(IndexHeader)>(pred));

    dariadb::Time res = dariadb::MAX_TIME;
    for (auto pname : page_list) {
      auto ih = Page::readIndexHeader(PageIndex::index_name_from_page_name(pname));
      res = std::min(ih.minTime, res);
    }

    return res;
  }
  dariadb::Time maxTime() {

    auto pred = [](const IndexHeader &) { return true; };

    auto page_list = pages_by_filter(std::function<bool(IndexHeader)>(pred));

    dariadb::Time res = dariadb::MIN_TIME;
    for (auto pname : page_list) {
      auto ih = Page::readIndexHeader(PageIndex::index_name_from_page_name(pname));
      res = std::max(ih.maxTime, res);
    }

    return res;
  }

  void append(const std::string &file_prefix, const dariadb::Meas::MeasArray &ma) {
    TIMECODE_METRICS(ctmd, "append", "PageManager::append(array)");
    if (!dariadb::utils::fs::path_exists(Options::instance()->path)) {
      dariadb::utils::fs::mkdir(Options::instance()->path);
    }

    Page *res = nullptr;

    std::string page_name = file_prefix + PAGE_FILE_EXT;
    std::string file_name =
        dariadb::utils::fs::append_path(Options::instance()->path, page_name);
    res = Page::create(file_name, last_id, Options::instance()->page_chunk_size, ma);
    Manifest::instance()->page_append(page_name);
    if (update_id) {
      res->header->max_chunk_id = last_id;
    }
    delete res;
    _file2header[page_name] =
        Page::readIndexHeader(PageIndex::index_name_from_page_name(file_name));
  }

protected:
  Page_Ptr _cur_page;
  mutable std::mutex _page_open_lock;

  uint64_t last_id;
  bool update_id;
  utils::LRU<std::string, Page_Ptr> _openned_pages;
  File2PageHeader _file2header;
};

PageManager::PageManager() : impl(new PageManager::Private) {}

PageManager::~PageManager() {}

void PageManager::start() {
  if (PageManager::_instance == nullptr) {
    PageManager::_instance = new PageManager();
  }
}

void PageManager::stop() {
  if (_instance != nullptr) {
    delete PageManager::_instance;
    _instance = nullptr;
  }
}

void PageManager::flush() {
  TIMECODE_METRICS(ctmd, "flush", "PageManager::flush");
  this->impl->flush();
}

PageManager *PageManager::instance() {
  return _instance;
}

bool PageManager::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                             dariadb::Time *maxResult) {
  return impl->minMaxTime(id, minResult, maxResult);
}

ChunkLinkList PageManager::chunksByIterval(const QueryInterval &query) {
  return impl->chunksByIterval(query);
}

dariadb::Meas::Id2Meas PageManager::valuesBeforeTimePoint(const QueryTimePoint &q) {
  return impl->valuesBeforeTimePoint(q);
}

void PageManager::readLinks(const QueryInterval &query, const ChunkLinkList &links,
                            IReaderClb *clb) {
  impl->readLinks(query, links, clb);
}

size_t PageManager::files_count() const {
  return impl->files_count();
}

size_t PageManager::chunks_in_cur_page() const {
  return impl->chunks_in_cur_page();
}

dariadb::Time PageManager::minTime() {
  return impl->minTime();
}

dariadb::Time PageManager::maxTime() {
  return impl->maxTime();
}

void PageManager::append(const std::string &file_prefix,
                         const dariadb::Meas::MeasArray &ma) {
  return impl->append(file_prefix, ma);
}
void PageManager::fsck(bool force_check) {
  return impl->fsck(force_check);
}

void PageManager::erase(const std::string &fname) {
  return impl->erase(fname);
}
