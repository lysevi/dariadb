#ifdef MSVC
    #define _SCL_SECURE_NO_WARNINGS //for stx::btree in msvc build.
#endif
#include <libdariadb/storage/page_manager.h>
#include <libdariadb/flags.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/locker.h>
#include <libdariadb/utils/metrics.h>
#include <libdariadb/utils/thread_manager.h>
#include <libdariadb/utils/utils.h>
#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/storage/page.h>
#include <libdariadb/storage/lock_manager.h>

#include <atomic>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <thread>

#include <stx/btree_multimap.h>

using namespace dariadb::storage;
using namespace dariadb::utils::async;

struct PageHeaderDescription {
	std::string path;
	IndexHeader hdr;
};

using File2PageHeader = stx::btree_multimap<dariadb::Time, PageHeaderDescription>;

class PageManager::Private {
public:
  Private(const EngineEnvironment_ptr env)
      : _cur_page(nullptr) {

	  _env = env;
	  _settings = _env->getResourceObject<Settings>(EngineEnvironment::Resource::SETTINGS);
    
    last_id = 0;

    if (utils::fs::path_exists(_settings->path)) {
      auto pages = _env->getResourceObject<Manifest>(EngineEnvironment::Resource::MANIFEST)->page_list();

      for (auto n : pages) {
        auto file_name = utils::fs::append_path(_settings->path, n);
		auto phdr = Page::readHeader(file_name);
		last_id = std::max(phdr.max_chunk_id, last_id);

        auto ihdr = Page::readIndexHeader(PageIndex::index_name_from_page_name(file_name));
		insert_pagedescr(n, ihdr);
      }
    }
  }

  ~Private() {
    if (_cur_page != nullptr) {
      _cur_page = nullptr;
    }
  }

  void fsck(bool force_check) {
    if (force_check) {
      logger_info("engine: PageManager fsck force.");
    }
    if (!utils::fs::path_exists(_settings->path)) {
      return;
    }

    auto pages = _env->getResourceObject<Manifest>(EngineEnvironment::Resource::MANIFEST)->page_list();

	for (auto n : pages) {
		auto file_name = utils::fs::append_path(_settings->path, n);
		auto hdr = Page::readHeader(file_name);
		if (hdr.removed_chunks == hdr.addeded_chunks) {
			logger_info("engine: page ", file_name, " is empty.");
			erase_page(file_name);
		}
		else {
			auto index_filename = PageIndex::index_name_from_page_name(n);
			auto index_file_path = utils::fs::append_path(_settings->path, index_filename);
			if (!utils::fs::path_exists(index_file_path)) {
				Page::restoreIndexFile(file_name);
			}
			if (force_check || (!hdr.is_closed && hdr.is_open_to_write)) {
				auto res = Page_Ptr{ Page::open(file_name) };
				res->fsck();
				res = nullptr;
			}
		}

	}
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
		  //TODO must be reading from index.
        TKIND_CHECK(THREAD_COMMON_KINDS::DISK_IO, ti.kind);
        Page_Ptr pg = open_page_to_read(pname);
        dariadb::Time lmin, lmax;
        if (pg->minMaxTime(id, &lmin, &lmax)) {
          results[num] = MMRes(true, lmin, lmax);
        } else {
          results[num] = MMRes(false, lmin, lmax);
        }
      };
      task_res[num] =
          ThreadManager::instance()->post(THREAD_COMMON_KINDS::DISK_IO, AT(at));
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

  Page_Ptr open_page_to_read(const std::string &pname)const {
    TIMECODE_METRICS(ctmd, "open", "PageManager::open_page_to_read");
    std::lock_guard<std::mutex> lg(_page_open_lock);
    Page_Ptr pg = nullptr;
    if (_cur_page != nullptr && pname == _cur_page->filename) {
      pg = _cur_page;
    } else {
        pg = Page_Ptr{Page::open(pname, true)};
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
	AsyncTask at = [query, &clb, &links, this](const ThreadInfo &ti) {
		TKIND_CHECK(THREAD_COMMON_KINDS::DISK_IO, ti.kind);
		ChunkLinkList to_read;

		for (auto l : links) {
			if (to_read.empty()) {
				to_read.push_back(l);
			}
			else {
				if (l.page_name == to_read.front().page_name) {
					to_read.push_back(l);
				}
				else {
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
	};
	auto pm_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::DISK_IO, AT(at));
	pm_async->wait();
  }

  std::list<std::string> pages_by_filter(std::function<bool(const IndexHeader &)> pred) {
    TIMECODE_METRICS(ctmd, "read", "PageManager::pages_by_filter");
    std::list<std::string> result;

    for (auto f2h : _file2header) {
      auto hdr = f2h.second.hdr;
      if (pred(hdr)) {
        auto page_file_name =
            utils::fs::append_path(_settings->path, f2h.second.path);
        result.push_back(page_file_name);
      }
    }
    return result;
  }

  Id2Meas valuesBeforeTimePoint(const QueryTimePoint &query) {
    TIMECODE_METRICS(ctmd, "readTimePoint", "PageManager::valuesBeforeTimePoint");

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

    Id2Meas result;

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
  size_t files_count() const { return _env->getResourceObject<Manifest>(EngineEnvironment::Resource::MANIFEST)->page_list().size(); }

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

  void append(const std::string &file_prefix, const dariadb::MeasArray &ma) {
    TIMECODE_METRICS(ctmd, "append", "PageManager::append(array)");
    if (!dariadb::utils::fs::path_exists(_settings->path)) {
      dariadb::utils::fs::mkdir(_settings->path);
    }

    Page *res = nullptr;

    std::string page_name = file_prefix + PAGE_FILE_EXT;
    std::string file_name =
        dariadb::utils::fs::append_path(_settings->path, page_name);
    res = Page::create(file_name, last_id, _settings->chunk_size.value, ma);
	_env->getResourceObject<Manifest>(EngineEnvironment::Resource::MANIFEST)->page_append(page_name);
	last_id=res->header->max_chunk_id;
    delete res;

	insert_pagedescr(page_name, Page::readIndexHeader(PageIndex::index_name_from_page_name(file_name)));
  }

  static void erase(const std::string& storage_path,const std::string &fname) {
      auto full_file_name = utils::fs::append_path(storage_path, fname);
      utils::fs::rm(full_file_name);
      utils::fs::rm(PageIndex::index_name_from_page_name(full_file_name));
  }
  
  void erase_page(const std::string &full_file_name) {
	  auto fname = utils::fs::extract_filename(full_file_name);

	  _env->getResourceObject<Manifest>(EngineEnvironment::Resource::MANIFEST)->page_rm(fname);
	  utils::fs::rm(full_file_name);
	  utils::fs::rm(PageIndex::index_name_from_page_name(full_file_name));
	  auto it = _file2header.begin();
	  for (; it != _file2header.end(); ++it) {
		  if (it->second.path == fname) {
			  break;
		  }
	  }
	  _file2header.erase(it);
  }

  void eraseOld(const Time t) {
      TIMECODE_METRICS(ctmd, "eraseOld", "PageManager::eraseOld");


	  auto pred = [t](const IndexHeader &hdr) {
		  auto in_check = hdr.maxTime <= t;
		  return in_check;
	  };

	  auto page_list = pages_by_filter(std::function<bool(IndexHeader)>(pred));
	  for (auto&p : page_list) {
		  this->erase_page(p);
	  }
  }

  void compactTo(uint32_t pagesCount) {

	  auto pred = [](const IndexHeader &hdr) {
		  return true;
	  };

	  auto page_list = pages_by_filter(std::function<bool(IndexHeader)>(pred));
      auto in_one = (size_t)(float(page_list.size()) / pagesCount+1);
	  auto it = page_list.begin();
	  while (it != page_list.end()) {
		  std::list<std::string> part;
		  for (size_t i = 0; i < in_one; ++i) {
			  part.push_back(*it);
			  ++it;
			  if (it == page_list.end()) {
				  break;
			  }
		  }
		  compact(part);
	  }
  }
  void compactbyTime(Time from, Time to) {
	  auto pred = [from,to](const IndexHeader &hdr) {
		  return hdr.minTime>=from && hdr.maxTime <= to;
	  };

	  auto page_list = pages_by_filter(std::function<bool(IndexHeader)>(pred));
	  if (page_list.size() <= 1) {
		  logger_info("engine: compactbyTime - pages count le 1.");
		  return;
	  }
	  compact(page_list);
  }

  void compact(std::list<std::string> part) {
	  Page *res = nullptr;
	  std::string page_name = utils::fs::random_file_name(".page");
      logger_info("engine: compacting to ", page_name);
      for(auto&p:part){
          logger_info("==> ",utils::fs::extract_filename(p));
      }
	  std::string file_name =
		  dariadb::utils::fs::append_path(_settings->path, page_name);
	  res = Page::create(file_name, last_id, _settings->chunk_size.value, part);
	  _env->getResourceObject<Manifest>(EngineEnvironment::Resource::MANIFEST)->page_append(page_name);
	  last_id = res->header->max_chunk_id;
	  delete res;
	  for (auto erasedPage : part) {
		  this->erase_page(erasedPage);
	  }

	  insert_pagedescr(page_name, Page::readIndexHeader(PageIndex::index_name_from_page_name(file_name)));
  }

  void appendChunks(const std::vector<Chunk*>& a, size_t count) {
	  auto lm = this->_env->getResourceObject<LockManager>(EngineEnvironment::Resource::LOCK_MANAGER);
	  lm->lock(LOCK_KIND::EXCLUSIVE, { LOCK_OBJECTS::PAGE });
	  Page *res = nullptr;
	  std::string page_name = utils::fs::random_file_name(".page");
	  logger_info("engine: write chunks to ", page_name);
	  std::string file_name =
		  dariadb::utils::fs::append_path(_settings->path, page_name);
	  res = Page::create(file_name, last_id, a,count);
	  _env->getResourceObject<Manifest>(EngineEnvironment::Resource::MANIFEST)->page_append(page_name);
	  last_id = res->header->max_chunk_id;
	  delete res;

	  insert_pagedescr(page_name, Page::readIndexHeader(PageIndex::index_name_from_page_name(file_name)));
	  lm->unlock({ LOCK_OBJECTS::PAGE });
  }

  void insert_pagedescr(std::string page_name, IndexHeader hdr) {
	  PageHeaderDescription ph_d;
	  ph_d.hdr = hdr;
	  ph_d.path = page_name;
	  _file2header.insert(std::make_pair(ph_d.hdr.maxTime, ph_d));
  }

  Id2MinMax loadMinMax(){
      Id2MinMax result;

      TIMECODE_METRICS(ctmd, "minMaxTime", "PageManager::minMaxTime");

      auto pages = pages_by_filter(
          [](const IndexHeader &ih) { return true; });

      std::vector<Id2MinMax> sub_results{pages.size()};
      std::vector<TaskResult_Ptr> task_res{pages.size()};
      size_t num = 0;

      for (auto pname : pages) {
        AsyncTask at = [pname, &sub_results, num, this](const ThreadInfo &ti) {
          TKIND_CHECK(THREAD_COMMON_KINDS::DISK_IO, ti.kind);
          Page_Ptr pg = open_page_to_read(pname);

          sub_results[num] = pg->loadMinMax();

        };
        task_res[num] =
            ThreadManager::instance()->post(THREAD_COMMON_KINDS::DISK_IO, AT(at));
        num++;
      }

      for (auto &tw : task_res) {
        tw->wait();
      }

      for(auto r:sub_results){
          minmax_append(result,r);
      }

      return result;
  }
protected:
  Page_Ptr _cur_page;
  mutable std::mutex _page_open_lock;

  uint64_t last_id;
  File2PageHeader _file2header;
  EngineEnvironment_ptr _env;
  Settings* _settings;
};

PageManager::PageManager(const EngineEnvironment_ptr env) : impl(new PageManager::Private(env)) {}

PageManager::~PageManager() {}

void PageManager::flush() {
  TIMECODE_METRICS(ctmd, "flush", "PageManager::flush");
  this->impl->flush();
}

bool PageManager::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                             dariadb::Time *maxResult) {
  return impl->minMaxTime(id, minResult, maxResult);
}

ChunkLinkList PageManager::chunksByIterval(const QueryInterval &query) {
  return impl->chunksByIterval(query);
}

dariadb::Id2Meas PageManager::valuesBeforeTimePoint(const QueryTimePoint &q) {
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
                         const dariadb::MeasArray &ma) {
  return impl->append(file_prefix, ma);
}
void PageManager::fsck(bool force_check) {
  return impl->fsck(force_check);
}

void PageManager::eraseOld(const dariadb::Time t) {
	impl->eraseOld(t);
}

void PageManager::erase(const std::string& storage_path,const std::string &fname) {
  Private::erase(storage_path, fname);
}

void PageManager::erase_page(const std::string &fname) {
	impl->erase_page(fname);
}

void PageManager::compactTo(uint32_t pagesCount) {
	impl->compactTo(pagesCount);
}

void PageManager::compactbyTime(dariadb::Time from, dariadb::Time to) {
	impl->compactbyTime(from,to);
}

void PageManager::appendChunks(const std::vector<Chunk*>& a, size_t count){
	impl->appendChunks(a, count);
}

dariadb::Id2MinMax PageManager::loadMinMax(){
    return impl->loadMinMax();
}
