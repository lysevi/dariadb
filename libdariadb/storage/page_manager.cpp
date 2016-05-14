#include "page_manager.h"
#include "../utils/asyncworker.h"
#include "../utils/fs.h"
#include "../utils/locker.h"
#include "../utils/utils.h"
#include "manifest.h"
#include "page.h"

#include <condition_variable>
#include <cstring>
#include <queue>
#include <thread>

const std::string MANIFEST_FILE_NAME="Manifest";

using namespace dariadb::storage;
dariadb::storage::PageManager *PageManager::_instance = nullptr;
//PM
class PageManager::Private /*:public dariadb::utils::AsyncWorker<Chunk_Ptr>*/ {
public:
  Private(const PageManager::Params &param)
      : _cur_page(nullptr), _param(param),_manifest(utils::fs::append_path(param.path,MANIFEST_FILE_NAME)) {
    /*this->start_async();*/
  }

  ~Private() {
   /* this->stop_async();*/

    if (_cur_page != nullptr) {
      delete _cur_page;
      _cur_page = nullptr;
    }
  }

  uint64_t calc_page_size() const {
    auto sz_info = _param.chunk_per_storage * sizeof(ChunkIndexInfo);
    auto sz_buffers = _param.chunk_per_storage * _param.chunk_size;
    return sizeof(PageHeader) + sz_buffers + sz_info;
  }

  Page *create_page() {
      if (!dariadb::utils::fs::path_exists(_param.path)) {
          dariadb::utils::fs::mkdir(_param.path);
      }

      Page *res = nullptr;

      auto names=_manifest.page_list();
      for(auto n:names){
          auto file_name =
                  utils::fs::append_path(_param.path,n);
          auto hdr=Page::readHeader(file_name);
          if(!hdr.is_full){
              res = Page::open(file_name);
          }
      }
      if(res==nullptr){
          std::string page_name = utils::fs::random_file_name(".page");
          std::string file_name =
                  dariadb::utils::fs::append_path(_param.path, page_name);
          auto sz = calc_page_size();
          res = Page::create(file_name, sz, _param.chunk_per_storage,
                             _param.chunk_size);
          _manifest.page_append(page_name);
      }

      return res;
  }
  //PM
  void flush() { /*this->flush_async();*/ }
  //void call_async(const Chunk_Ptr &ch) override { /*write_to_page(ch);*/ }

  Page *get_cur_page() {
    if (_cur_page == nullptr) {
      _cur_page = create_page();
    }
    return _cur_page;
  }
  //PM
  /*bool write_to_page(const Chunk_Ptr &ch) {
    std::lock_guard<std::mutex> lg(_locker_write);
    auto pg = get_cur_page();
    return pg->append(ch);
  }*/

  /*bool append(const Chunk_Ptr &ch) {
    std::lock_guard<std::mutex> lg(_locker);
    this->add_async_data(ch);
    return true;
  }

  bool append(const ChunksList &lst) {
    for (auto &c : lst) {
      if (!append(c)) {
        return false;
      }
    }
    return true;
  }*/

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) {
    std::lock_guard<std::mutex> lg(_locker);
    auto p = get_cur_page();
    return p->minMaxTime(id, minResult, maxResult);
  }

  Cursor_ptr chunksByIterval(const QueryInterval &query) {
    std::lock_guard<std::mutex> lg(_locker);
    auto p = get_cur_page();
    return p->chunksByIterval(query);
  }

  IdToChunkMap chunksBeforeTimePoint(const QueryTimePoint &q) {
    std::lock_guard<std::mutex> lg(_locker);

    auto cur_page = this->get_cur_page();

    return cur_page->chunksBeforeTimePoint(q);
  }

  dariadb::IdArray getIds() {
    std::lock_guard<std::mutex> lg(_locker);
    if (_cur_page == nullptr) {
      return dariadb::IdArray{};
    }
    auto cur_page = this->get_cur_page();
    return cur_page->getIds();
  }

//  dariadb::storage::ChunksList get_open_chunks() {
//    if (!dariadb::utils::fs::path_exists(_param.path)) {
//      return ChunksList{};
//    }
//    return this->get_cur_page()->get_open_chunks();
//  }

  size_t chunks_in_cur_page() const {
    if (_cur_page == nullptr) {
      return 0;
    }
    return _cur_page->header->addeded_chunks;
  }
  //PM
  size_t in_queue_size() const { return 0;/*return this->async_queue_size();*/ }

  dariadb::Time minTime() {
    std::lock_guard<std::mutex> lg(_locker);
    if (_cur_page == nullptr) {
      return dariadb::Time(0);
    } else {
      return _cur_page->iheader->minTime;
    }
  }

  dariadb::Time maxTime() {
    std::lock_guard<std::mutex> lg(_locker);
    if (_cur_page == nullptr) {
      return dariadb::Time(0);
    } else {
      return _cur_page->iheader->maxTime;
    }
  }

  append_result append(const Meas & value) {
	  std::lock_guard<std::mutex> lg(_locker);
      while(true){
          auto cur_page = this->get_cur_page();
          auto res=cur_page->append(value);
          if(res.writed!=1){
              cur_page=nullptr;
          }else{
              return res;
          }
      }
  }
protected:
  Page *_cur_page;
  PageManager::Params _param;
  Manifest _manifest;
  std::mutex _locker, _locker_write;
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
//PM
//bool PageManager::append(const Chunk_Ptr &c) {
//  return impl->append(c);
//}
//
//bool PageManager::append(const ChunksList &c) {
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

dariadb::storage::Cursor_ptr
PageManager::chunksByIterval(const QueryInterval &query) {
  return impl->chunksByIterval(query);
}

dariadb::storage::IdToChunkMap
PageManager::chunksBeforeTimePoint(const QueryTimePoint &q) {
  return impl->chunksBeforeTimePoint(q);
}

dariadb::IdArray PageManager::getIds() {
  return impl->getIds();
}

//dariadb::storage::ChunksList PageManager::get_open_chunks() {
//  return impl->get_open_chunks();
//}

size_t PageManager::chunks_in_cur_page() const {
  return impl->chunks_in_cur_page();
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

dariadb::append_result dariadb::storage::PageManager::append(const Meas & value){
	return impl->append(value);
}
