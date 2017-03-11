#ifdef MSVC
#define _SCL_SECURE_NO_WARNINGS // for stx::btree in msvc build.
#endif
#include <libdariadb/flags.h>
#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/cursors.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/storage/pages/page.h>
#include <libdariadb/storage/pages/page_manager.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/async/locker.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/utils.h>

#include <atomic>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

#include <stx/btree_multimap.h>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils::async;

struct PageFooterDescription {
  std::string path;
  IndexFooter hdr;
};

using File2PageFooter = stx::btree_multimap<dariadb::Time, PageFooterDescription>;

class PageManager::Private {
public:
  Private(const EngineEnvironment_ptr env) : _cur_page(nullptr) {

    _env = env;
    _settings = _env->getResourceObject<Settings>(EngineEnvironment::Resource::SETTINGS);
    _manifest = _env->getResourceObject<Manifest>(EngineEnvironment::Resource::MANIFEST);
    last_id = 0;
    reloadIndexFooters();
  }

  void reloadIndexFooters() {
    if (utils::fs::path_exists(_settings->raw_path.value())) {
      _file2footer.clear();
      auto pages = _manifest->page_list();

      for (auto n : pages) {
        auto file_name = utils::fs::append_path(_settings->raw_path.value(), n);
        auto phdr = Page::readFooter(file_name);
        last_id = std::max(phdr.max_chunk_id, last_id);

        auto index_filename = PageIndex::index_name_from_page_name(file_name);
        if (utils::fs::file_exists(index_filename)) {
          auto ihdr = Page::readIndexFooter(index_filename);
          insert_pagedescr(n, ihdr);
        }
      }
    }
  }

  ~Private() {
    if (_cur_page != nullptr) {
      _cur_page = nullptr;
    }
  }

  void fsck() {
    if (!utils::fs::path_exists(_settings->raw_path.value())) {
      return;
    }

    auto pages = _manifest->page_list();

    for (auto n : pages) {
      auto file_name = utils::fs::append_path(_settings->raw_path.value(), n);
      try {
        auto index_filename = PageIndex::index_name_from_page_name(n);
        auto index_file_path =
            utils::fs::append_path(_settings->raw_path.value(), index_filename);

        if (!utils::fs::path_exists(index_file_path)) {
          Page::restoreIndexFile(file_name);
        } else {
          auto ifooter = PageIndex::readIndexFooter(index_file_path);
          if (!ifooter.check()) {
            utils::fs::rm(index_file_path);
            Page::restoreIndexFile(file_name);
          }
        }
        Page_Ptr p{Page::open(file_name)};

        if (!p->checksum()) {
          logger_info("engine", _settings->alias, ": checksum of page ", file_name,
                      " is wrong - removing.");
          erase_page(file_name);
        } else {
          if (!p->footer.check()) {
            logger_info("engine", _settings->alias, ": bad magic nums ", file_name);
            erase_page(file_name);
          }
        }
      } catch (std::exception &ex) {
        logger_fatal("engine", _settings->alias, ": error on check ", file_name, ": ",
                     ex.what());
        erase_page(file_name);
      }
    }
    reloadIndexFooters();
  }

  // PM
  void flush() {}

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult, dariadb::Time *maxResult) {
    auto pages = pages_by_filter(
        [id](const IndexFooter &ih) { return (storage::bloom_check(ih.id_bloom, id)); });

    using MMRes = std::tuple<bool, dariadb::Time, dariadb::Time>;
    std::vector<MMRes> results{pages.size()};
    std::vector<TaskResult_Ptr> task_res{pages.size()};
    size_t num = 0;

    for (auto pname : pages) {
      AsyncTask at = [pname, &results, num, this, id](const ThreadInfo &ti) {
        /// by fact, reading from index.
        TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);
        Page_Ptr pg = open_page_to_read(pname);
        dariadb::Time lmin, lmax;
        if (pg->minMaxTime(id, &lmin, &lmax)) {
          results[num] = MMRes(true, lmin, lmax);
        } else {
          results[num] = MMRes(false, lmin, lmax);
        }
        return false;
      };
      task_res[num] = ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
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

  Page_Ptr open_page_to_read(const std::string &pname) const {
    std::lock_guard<std::mutex> lg(_page_open_lock);
    Page_Ptr pg = nullptr;
    if (_cur_page != nullptr && pname == _cur_page->filename) {
      pg = _cur_page;
    } else {
      pg = Page_Ptr{Page::open(pname)};
    }
    return pg;
  }

  Statistic stat(const Id &id, Time from, Time to) {
    auto pred = [id, from, to](const IndexFooter &hdr) {
      auto interval_check((hdr.stat.minTime >= from && hdr.stat.maxTime <= to) ||
                          (utils::inInterval(from, to, hdr.stat.minTime)) ||
                          (utils::inInterval(from, to, hdr.stat.maxTime)) ||
                          (utils::inInterval(hdr.stat.minTime, hdr.stat.maxTime, from)) ||
                          (utils::inInterval(hdr.stat.minTime, hdr.stat.maxTime, to)));
      if (interval_check) {
        if (storage::bloom_check(hdr.id_bloom, id)) {
          return true;
        }
      }
      return false;
    };
    Statistic result;

    AsyncTask at = [id, from, to, pred, this, &result](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);
      ChunkLinkList to_read;

      auto page_list = pages_by_filter(std::function<bool(const IndexFooter &)>(pred));

      for (auto pname : page_list) {
        auto p = Page::open(pname);
        auto sub_result = p->stat(id, from, to);
        result.update(sub_result);
      }

      return false;
    };
    auto pm_async = ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
    pm_async->wait();
    return result;
  }

  Id2Cursor intervalReader(const QueryInterval &query) {
    Id2CursorsList result;

    AsyncTask at = [&query, this, &result](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);
      this->callback_for_interval_readers(query, result);

      return false;
    };
    auto pm_async = ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
    pm_async->wait();
    return CursorWrapperFactory::colapseCursors(result);
  }

  void callback_for_interval_readers(const QueryInterval &query, Id2CursorsList &result) {
    auto pred = [&query](const IndexFooter &hdr) {
      auto interval_check(
          (hdr.stat.minTime >= query.from && hdr.stat.maxTime <= query.to) ||
          (utils::inInterval(query.from, query.to, hdr.stat.minTime)) ||
          (utils::inInterval(query.from, query.to, hdr.stat.maxTime)) ||
          (utils::inInterval(hdr.stat.minTime, hdr.stat.maxTime, query.from)) ||
          (utils::inInterval(hdr.stat.minTime, hdr.stat.maxTime, query.to)));
      if (interval_check) {
        for (auto id : query.ids) {
          if (storage::bloom_check(hdr.id_bloom, id) &&
              (query.flag == Flag(0) ||
               storage::bloom_check(hdr.stat.flag_bloom, query.flag))) {
            return true;
          }
        }
      }
      return false;
    };
    auto page_list = pages_by_filter(std::function<bool(const IndexFooter &)>(pred));

    for (auto pname : page_list) {
      auto p = Page::open(pname);
      auto sub_result = p->intervalReader(query);
      for (auto kv : sub_result) {
        result[kv.first].push_back(kv.second);
      }
    }
  }

  std::list<std::string> pages_by_filter(std::function<bool(const IndexFooter &)> pred) {
    std::list<PageFooterDescription> sub_result;

    for (auto f2h : _file2footer) {
      auto hdr = f2h.second.hdr;
      if (pred(hdr)) {

        sub_result.push_back(f2h.second);
      }
    }

    std::vector<PageFooterDescription> vec_res{sub_result.begin(), sub_result.end()};
    std::sort(vec_res.begin(), vec_res.end(),
              [](auto lr, auto rr) { return lr.hdr.stat.minTime < rr.hdr.stat.minTime; });
    std::list<std::string> result;
    for (auto hd : vec_res) {
      auto page_file_name = utils::fs::append_path(_settings->raw_path.value(), hd.path);
#ifdef DOUBLE_CHECKS
      if(!utils::fs::file_exists(page_file_name)){
          THROW_EXCEPTION("page no exists ",page_file_name);
      }
#endif
      result.push_back(page_file_name);
    }
#ifdef DEBUG
    std::list<Time> tm;
    for (auto pname : result) {
      auto pi = PageIndex::open(PageIndex::index_name_from_page_name(pname));
      if (!tm.empty()) {
        if (pi->iheader.stat.minTime < tm.back()) {
          THROW_EXCEPTION("logic_error");
        }
      }
      tm.push_back(pi->iheader.stat.minTime);
    }
#endif
    return result;
  }

  Id2Meas valuesBeforeTimePoint(const QueryTimePoint &query) {
    Id2Meas result;

    for (auto id : query.ids) {
      result[id].flag = FLAGS::_NO_DATA;
      result[id].time = query.time_point;
    }

    AsyncTask at = [&query, &result, this](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);
      auto pred = [query](const IndexFooter &hdr) {
        auto in_check =
            utils::inInterval(hdr.stat.minTime, hdr.stat.maxTime, query.time_point) ||
            (hdr.stat.maxTime < query.time_point);
        if (in_check) {
          for (auto id : query.ids) {
            if (storage::bloom_check(hdr.id_bloom, id)) {
              return true;
            }
          }
        }
        return false;
      };

      auto page_list = pages_by_filter(std::function<bool(IndexFooter)>(pred));

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
      return false;
    };
    auto pm_async = ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
    pm_async->wait();
    return result;
  }

  size_t chunks_in_cur_page() const {
    if (_cur_page == nullptr) {
      return 0;
    }
    return _cur_page->footer.addeded_chunks;
  }
  // PM
  size_t files_count() const { return _manifest->page_list().size(); }

  dariadb::Time minTime() {

    auto pred = [](const IndexFooter &) { return true; };

    auto page_list = pages_by_filter(std::function<bool(IndexFooter)>(pred));

    dariadb::Time res = dariadb::MAX_TIME;
    for (auto pname : page_list) {
      auto ih = Page::readIndexFooter(PageIndex::index_name_from_page_name(pname));
      res = std::min(ih.stat.minTime, res);
    }

    return res;
  }
  dariadb::Time maxTime() {

    auto pred = [](const IndexFooter &) { return true; };

    auto page_list = pages_by_filter(std::function<bool(IndexFooter)>(pred));

    dariadb::Time res = dariadb::MIN_TIME;
    for (auto pname : page_list) {
      auto ih = Page::readIndexFooter(PageIndex::index_name_from_page_name(pname));
      res = std::max(ih.stat.maxTime, res);
    }

    return res;
  }

  // from wall
  void append(const std::string &file_prefix, const dariadb::SplitedById &ma) {
    if (!dariadb::utils::fs::path_exists(_settings->raw_path.value())) {
      dariadb::utils::fs::mkdir(_settings->raw_path.value());
    }

    Page_Ptr res = nullptr;

    std::string page_name = file_prefix + PAGE_FILE_EXT;
    std::string file_name =
        dariadb::utils::fs::append_path(_settings->raw_path.value(), page_name);
    res = Page::create(file_name, MIN_LEVEL, last_id, _settings->chunk_size.value(), ma);
    _manifest->page_append(page_name);
    last_id = res->footer.max_chunk_id;

    insert_pagedescr(page_name, Page::readIndexFooter(
                                    PageIndex::index_name_from_page_name(file_name)));
  }

  static void erase(const std::string &storage_path, const std::string &fname) {
      logger("pm: erase ",fname);
    auto full_file_name = utils::fs::append_path(storage_path, fname);
    auto ifull_name=PageIndex::index_name_from_page_name(full_file_name);
    utils::fs::rm(full_file_name);
    utils::fs::rm(ifull_name);
    ENSURE(!utils::fs::file_exists(full_file_name));
    ENSURE(!utils::fs::file_exists(ifull_name));
  }

  void erase_page(const std::string &full_file_name) {
      logger("pm: erase ",full_file_name);
    auto fname = utils::fs::extract_filename(full_file_name);
    auto ifull_name=PageIndex::index_name_from_page_name(full_file_name);

#ifdef DOUBLE_CHECKS
    auto pages_before=_file2footer.size();
#endif
    ENSURE(utils::fs::file_exists(full_file_name));
    _manifest->page_rm(fname);

    auto it = _file2footer.begin();
    for (; it != _file2footer.end(); ++it) {
#ifdef DOUBLE_CHECKS
        auto full_path=utils::fs::append_path(_settings->raw_path.value(),it->second.path);
      if(!utils::fs::file_exists(full_path)){
          THROW_EXCEPTION("page no exists ",full_path);
      }
#endif
      if (it->second.path == fname) {
        break;
      }
    }
    _file2footer.erase(it);

    utils::fs::rm(full_file_name);
    utils::fs::rm(ifull_name);

    ENSURE(!utils::fs::file_exists(full_file_name));
    ENSURE(!utils::fs::file_exists(ifull_name));

#ifdef DOUBLE_CHECKS
    auto pages_after=_file2footer.size();
    ENSURE(pages_before>pages_after);
#endif
  }

  void eraseOld(const Time t) {
      logger("pm: erase old");
    auto page_list = pagesOlderThan(t);
    for (auto &p : page_list) {
      this->erase_page(p);
    }
  }

  std::list<std::string> pagesOlderThan(Time t) {
    auto pred = [t](const IndexFooter &hdr) {
      auto in_check = hdr.stat.maxTime <= t;
      return in_check;
    };
    return pages_by_filter(std::function<bool(IndexFooter)>(pred));
  }

  void repack() {
    auto max_files_per_level = _settings->max_pages_in_level.value();

    for (uint16_t level = MIN_LEVEL; level < MAX_LEVEL; ++level) {
      auto pred = [level](const IndexFooter &hdr) { return hdr.level == level; };

      auto page_list = pages_by_filter(std::function<bool(IndexFooter)>(pred));

      while (page_list.size() > max_files_per_level) { // while level is filled
        std::list<std::string> part;
        for (size_t i = 0; i < max_files_per_level; ++i) {
          if (page_list.empty()) {
            break;
          }
          part.push_back(page_list.front());
          page_list.pop_front();
        }
        if (part.size() < size_t(2)) {
          break;
        }
        repack(level + 1, part);
      }
    }
  }

  void repack(uint16_t out_lvl, std::list<std::string> part) {
    Page_Ptr res = nullptr;
    std::string page_name = utils::fs::random_file_name(".page");
    logger_info("engine", _settings->alias, ": repack to level ", out_lvl,
                " page: ", page_name);
    for (auto &p : part) {
      logger_info("==> ", utils::fs::extract_filename(p));
    }
    auto start_time = clock();
    std::string file_name =
        dariadb::utils::fs::append_path(_settings->raw_path.value(), page_name);
    res = Page::repackTo(file_name, out_lvl, last_id, _settings->chunk_size.value(), part,
                         nullptr);
    _manifest->page_append(page_name);
    if (res != nullptr) {
      last_id = res->footer.max_chunk_id;
    }
    for (auto erasedPage : part) {
      this->erase_page(erasedPage);
    }
    if (res != nullptr) {
      insert_pagedescr(page_name, Page::readIndexFooter(
                                      PageIndex::index_name_from_page_name(file_name)));
    }
    auto elapsed = double(clock() - start_time) / CLOCKS_PER_SEC;

    logger("engine", _settings->alias, ": repack end. elapsed ", elapsed, "s");
  }

  void compact(ICompactionController *logic) {
    Time from = logic->from;
    Time to = logic->to;
    logger("engine", _settings->alias, ": compact. from ", from, " to ", to);
    auto start_time = clock();
    auto pred = [from, to](const IndexFooter &hdr) {
      return utils::inInterval(from, to, hdr.stat.minTime) ||
             utils::inInterval(from, to, hdr.stat.maxTime) ||
             utils::inInterval(hdr.stat.minTime, hdr.stat.maxTime, from) ||
             utils::inInterval(hdr.stat.minTime, hdr.stat.maxTime, to);
    };

    auto page_list_in_period = pages_by_filter(std::function<bool(IndexFooter)>(pred));
    if (page_list_in_period.empty()) {
      logger_info("engine", _settings->alias, ": compact. pages not found for interval");
      return;
    }

    auto timepoint = dariadb::timeutil::current_time();
    std::list<std::string> page_list;

    for (auto p : page_list_in_period) {
      auto ftr = Page::readFooter(p);
      if (timepoint - ftr.stat.maxTime >= logic->eraseOlderThan) {
        erase_page(p);
      } else {
        page_list.push_back(p);
      }
    }

    uint16_t level = 0;
    for (auto p : page_list) {
      logger_info("==> ", utils::fs::extract_filename(p));
      auto ftr = Page::readFooter(p);
      level = std::max(ftr.level, level);
    }

    Page_Ptr res = nullptr;
    std::string page_name = utils::fs::random_file_name(".page");

    std::string file_name =
        dariadb::utils::fs::append_path(_settings->raw_path.value(), page_name);
    res = Page::repackTo(file_name, level, last_id, _settings->chunk_size.value(),
                         page_list, logic);
    if (res != nullptr) {
      last_id = res->footer.max_chunk_id;
    }
    for (auto erasedPage : page_list) {
      this->erase_page(erasedPage);
    }
    if (res != nullptr) {
      insert_pagedescr(page_name, Page::readIndexFooter(
                                      PageIndex::index_name_from_page_name(file_name)));
    }
    auto elapsed = double(clock() - start_time) / CLOCKS_PER_SEC;
    logger("engine", _settings->alias, ": compact end. elapsed ", elapsed);
  }

  void appendChunks(const std::vector<Chunk *> &a, size_t count) {
    std::vector<Chunk *> tmp_buffer;
    tmp_buffer.resize(a.size());

    int64_t left = (int64_t)a.size();
    auto max_chunks = (int64_t)_settings->max_chunks_per_page.value();
    size_t pos_in_a = 0;
    while (left != 0) {
      std::string page_name = utils::fs::random_file_name(".page");
      logger_info("engine", _settings->alias, ": write chunks to ", page_name);
      size_t to_write = 0;
      if (max_chunks < left) {
        to_write = max_chunks;
        left -= to_write;
      } else {
        to_write = left;
        left = 0;
      }

      std::string file_name =
          dariadb::utils::fs::append_path(_settings->raw_path.value(), page_name);

      for (size_t i = 0; i < to_write; ++i) {
        tmp_buffer[i] = a[pos_in_a++];
      }

      auto res = Page::create(file_name, MIN_LEVEL, last_id, tmp_buffer, to_write);
      _manifest->page_append(page_name);
      last_id = res->footer.max_chunk_id;

      insert_pagedescr(page_name, Page::readIndexFooter(
                                      PageIndex::index_name_from_page_name(file_name)));
    }
  }

  void insert_pagedescr(std::string page_name, IndexFooter hdr) {
    PageFooterDescription ph_d;
    ph_d.hdr = hdr;
    ph_d.path = page_name;
    _file2footer.insert(std::make_pair(ph_d.hdr.stat.maxTime, ph_d));
  }

  Id2MinMax loadMinMax() {
    Id2MinMax result;

    auto pages = pages_by_filter([](const IndexFooter &ih) { return true; });

    AsyncTask at = [&result, &pages, this](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);
      for (auto pname : pages) {
        Page_Ptr pg = open_page_to_read(pname);

        auto sub_results = pg->loadMinMax();
        minmax_append(result, sub_results);
      }
      return false;
    };
    auto at_as = ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));

    at_as->wait();

    return result;
  }

protected:
  Page_Ptr _cur_page;
  mutable std::mutex _page_open_lock;

  uint64_t last_id;
  File2PageFooter _file2footer;
  EngineEnvironment_ptr _env;
  Settings *_settings;
  Manifest *_manifest;
};

PageManager_ptr PageManager::create(const EngineEnvironment_ptr env) {
  return PageManager_ptr{new PageManager(env)};
}

PageManager::PageManager(const EngineEnvironment_ptr env)
    : impl(new PageManager::Private(env)) {}

PageManager::~PageManager() {}

void PageManager::flush() {
  this->impl->flush();
}

bool PageManager::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                             dariadb::Time *maxResult) {
  return impl->minMaxTime(id, minResult, maxResult);
}

dariadb::Id2Meas PageManager::valuesBeforeTimePoint(const QueryTimePoint &q) {
  return impl->valuesBeforeTimePoint(q);
}

dariadb::Id2Cursor PageManager::intervalReader(const QueryInterval &query) {
  return impl->intervalReader(query);
}

Statistic PageManager::stat(const Id id, Time from, Time to) {
  return impl->stat(id, from, to);
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

void PageManager::append(const std::string &file_prefix, const dariadb::SplitedById &ma) {
  return impl->append(file_prefix, ma);
}
void PageManager::fsck() {
  return impl->fsck();
}

void PageManager::eraseOld(const dariadb::Time t) {
  impl->eraseOld(t);
}

void PageManager::erase(const std::string &storage_path, const std::string &fname) {
  Private::erase(storage_path, fname);
}

void PageManager::erase_page(const std::string &fname) {
  impl->erase_page(fname);
}

std::list<std::string> PageManager::pagesOlderThan(Time t) {
  return impl->pagesOlderThan(t);
}

void PageManager::repack() {
  impl->repack();
}

void PageManager::compact(ICompactionController *logic) {
  impl->compact(logic);
}

void PageManager::appendChunks(const std::vector<Chunk *> &a, size_t count) {
  impl->appendChunks(a, count);
}

dariadb::Id2MinMax PageManager::loadMinMax() {
  return impl->loadMinMax();
}
