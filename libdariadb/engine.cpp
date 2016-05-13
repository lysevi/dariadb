#include "engine.h"
#include "storage/capacitor.h"
#include "storage/memstorage.h"
#include "storage/page_manager.h"
#include "utils/exception.h"
#include "utils/locker.h"
#include <algorithm>
#include <cassert>

using namespace dariadb;
using namespace dariadb::storage;

class Engine::Private {
public:
  Private(const PageManager::Params &page_storage_params,
          dariadb::storage::Capacitor::Params cap_params,
          dariadb::storage::Engine::Limits limits)
      : mem_storage{new MemoryStorage(page_storage_params.chunk_size)},
        _page_manager_params(page_storage_params), _cap_params(cap_params),
        _limits(limits) {
    dariadb::storage::ChunkPool::instance()->start();

    mem_cap = new Capacitor(mem_storage, _cap_params);
    mem_storage_raw = dynamic_cast<MemoryStorage *>(mem_storage.get());
    assert(mem_storage_raw != nullptr);

    PageManager::start(_page_manager_params);

    auto open_chunks = PageManager::instance()->get_open_chunks();
    mem_storage_raw->append(open_chunks);
    // mem_storage_raw->set_chunkWriter(PageManager::instance());
  }
  ~Private() {
    this->flush();
    if (_limits.max_mem_chunks != 0) {
      auto all_chunks = this->mem_storage_raw->drop_all();
      PageManager::instance()->append(all_chunks); // use specified in ctor
    }
    delete mem_cap;
    PageManager::stop();
  }

  Time minTime() {
    std::lock_guard<std::recursive_mutex> lg(_locker);
    if (PageManager::instance()->chunks_in_cur_page() > 0) {
      return PageManager::instance()->minTime();
    } else {
      return mem_storage->minTime();
    }
  }

  Time maxTime() { return mem_storage->maxTime(); }

  append_result append(const Meas &value) {
    append_result result{};
    if (mem_cap->append(value).writed!=1) {
      // if(mem_storage_raw->append(value).writed!=1){
      assert(false);
      result.ignored++;
    } else {
      result.writed++;
    }

    drop_old_chunks();
    return result;
  }

  void drop_old_chunks() {
    if (_limits.max_mem_chunks == 0) {
      if (_limits.old_mem_chunks != 0) {
        auto old_chunks =
            mem_storage_raw->drop_old_chunks(_limits.old_mem_chunks);
        PageManager::instance()->append(old_chunks);
      }
    } else {
      auto old_chunks =
          mem_storage_raw->drop_old_chunks_by_limit(_limits.max_mem_chunks);
      PageManager::instance()->append(old_chunks);
    }
  }

  void subscribe(const IdArray &ids, const Flag &flag,
                 const ReaderClb_ptr &clbk) {
    mem_storage->subscribe(ids, flag, clbk);
  }

  Reader_ptr currentValue(const IdArray &ids, const Flag &flag) {
    return mem_storage->currentValue(ids, flag);
  }

  void flush() {
    std::lock_guard<std::recursive_mutex> lg(_locker);
    this->mem_cap->flush();
    this->drop_old_chunks();
    PageManager::instance()->flush();
  }

  Engine::QueueSizes queue_size() const {
    QueueSizes result;
    result.page = PageManager::instance()->in_queue_size();
    result.mem = this->mem_storage_raw->queue_size();
    result.cap = this->mem_cap->in_queue_size();
    return result;
  }

  class UnionCursor : public Cursor {
  public:
    Cursor_ptr _page_cursor;
    Cursor_ptr _mem_cursor;
    UnionCursor(Cursor_ptr &page_cursor, Cursor_ptr &mem_cursor)
        : _page_cursor{page_cursor}, _mem_cursor(mem_cursor) {
      this->reset_pos();
    }
    ~UnionCursor() {
      _page_cursor = nullptr;
      _mem_cursor = nullptr;
    }

    bool is_end() const override {
      return (_page_cursor == nullptr ? true : _page_cursor->is_end()) &&
             (_mem_cursor == nullptr ? true : _mem_cursor->is_end());
    }

    void readNext(Cursor::Callback *cbk) override {
      if (!is_end()) {
        if ((_page_cursor != nullptr) && (!_page_cursor->is_end())) {
          _page_cursor->readNext(cbk);
          return;
        } else {
          if ((_mem_cursor != nullptr) && (!_mem_cursor->is_end())) {
            _mem_cursor->readNext(cbk);
            return;
          }
        }
      }
    }

    void reset_pos() override {
      if (_page_cursor != nullptr) {
        _page_cursor->reset_pos();
      }
      if (_mem_cursor != nullptr) {
        _mem_cursor->reset_pos();
      }
    }
  };

  class UnionCursorSet : public Cursor {
  public:
    CursorList _cursors;
    CursorList::iterator it;
    bool _is_end;
    UnionCursorSet() {}
    ~UnionCursorSet() { _cursors.clear(); }

    void add_cursor(const Cursor_ptr &cptr) {
      _cursors.push_back(cptr);
      reset_pos();
    }

    bool is_end() const override { return _is_end; }

    void readNext(Cursor::Callback *cbk) override {
      if (it == _cursors.end()) {
        _is_end = true;
        Chunk_Ptr empty;
        cbk->call(empty);
        return;
      } else {
        Cursor_ptr c = *it;
        if (c->is_end()) { // TODO refact.
          if (it == _cursors.end()) {
            _is_end = true;
            Chunk_Ptr empty;
            cbk->call(empty);
            return;
          } else {
            ++it;
          }
        }
        if (it != _cursors.end()) {
          c = *it;
          c->readNext(cbk);
        }
      }
      if (_is_end) {
        Chunk_Ptr empty;
        cbk->call(empty);
      }
    }

    void reset_pos() override {
      it = _cursors.begin();
      _is_end = false;
      for (auto &c : _cursors) {
        c->reset_pos();
      }
    }
  };

  Cursor_ptr chunksByIterval(const IdArray &ids, Flag flag, Time from,
                             Time to) {
    std::lock_guard<std::recursive_mutex> lg(_locker);
    IdArray id_a = ids;
    if (id_a.empty()) {
      id_a = getIds();
    }
    UnionCursorSet *raw_result = new UnionCursorSet();
    Cursor_ptr result{raw_result};
    for (auto id : id_a) {
      IdArray cur_ids(1);
      cur_ids[0] = id;
      Cursor_ptr page_chunks, mem_chunks;
      dariadb::Time minT, maxT;
      if (!mem_storage_raw->minMaxTime(id, &minT, &maxT)) {
        page_chunks =
            PageManager::instance()->chunksByIterval(cur_ids, flag, from, to);
      } else {
        if (minT <= from && maxT >= to) {
          mem_chunks =
              mem_storage_raw->chunksByIterval(cur_ids, flag, from, to);
        } else {
          page_chunks = PageManager::instance()->chunksByIterval(cur_ids, flag,
                                                                 from, minT);
          mem_chunks =
              mem_storage_raw->chunksByIterval(cur_ids, flag, minT, to);
        }
      }

      Cursor_ptr sub_result{new UnionCursor{page_chunks, mem_chunks}};
      raw_result->add_cursor(sub_result);
    }
    return result;
  }

  IdToChunkMap chunksBeforeTimePoint(const IdArray &ids, Flag flag,
                                     Time timePoint) {
    std::lock_guard<std::recursive_mutex> lg(_locker);
    IdArray id_a = ids;
    if (id_a.empty()) {
      id_a = getIds();
    }
    IdToChunkMap result;
    IdArray cur_ids{1};
    for (auto id : id_a) {
      cur_ids[0] = id;
      dariadb::Time minT, maxT;
      IdToChunkMap subRes;
      if (!mem_storage_raw->minMaxTime(id, &minT, &maxT)) {
        subRes = PageManager::instance()->chunksBeforeTimePoint(cur_ids, flag,
                                                                timePoint);
      } else {
        if (minT <= timePoint) {
          subRes =
              mem_storage_raw->chunksBeforeTimePoint(cur_ids, flag, timePoint);
        } else {
          subRes = PageManager::instance()->chunksBeforeTimePoint(cur_ids, flag,
                                                                  timePoint);
        }
      }
      for (auto kv : subRes) {
        result[kv.first] = kv.second;
      }
    }
    return result;
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) {
    dariadb::Time subMin1, subMax1;
    auto pr = PageManager::instance()->minMaxTime(id, &subMin1, &subMax1);
    dariadb::Time subMin2, subMax2;
    auto mr = mem_storage_raw->minMaxTime(id, &subMin2, &subMax2);

    if (!pr) {
      *minResult = subMin2;
      *maxResult = subMax2;
      return mr;
    }
    if (!mr) {
      *minResult = subMin1;
      *maxResult = subMax1;
      return pr;
    }
    *minResult = std::min(subMin1, subMin2);
    *maxResult = std::max(subMax1, subMax2);
    return true;
  }

  IdArray getIds() {
    std::lock_guard<std::recursive_mutex> lg(_locker);
    auto page_ids = PageManager::instance()->getIds();
    auto mem_ids = mem_storage_raw->getIds();
    dariadb::IdSet s;
    for (auto v : page_ids) {
      s.insert(v);
    }
    for (auto v : mem_ids) {
      s.insert(v);
    }
    return dariadb::IdArray{s.begin(), s.end()};
  }

  size_t chunks_in_memory() const {
    std::lock_guard<std::recursive_mutex> lg(_locker);
    return mem_storage_raw->chunks_total_size();
  }

  storage::BaseStorage_ptr mem_storage;
  storage::MemoryStorage *mem_storage_raw;
  storage::Capacitor *mem_cap;

  storage::PageManager::Params _page_manager_params;
  dariadb::storage::Capacitor::Params _cap_params;
  dariadb::storage::Engine::Limits _limits;
  mutable std::recursive_mutex _locker;
};

Engine::Engine(storage::PageManager::Params page_manager_params,
                           dariadb::storage::Capacitor::Params cap_params,
                           const dariadb::storage::Engine::Limits &limits)
    : _impl{
          new Engine::Private(page_manager_params, cap_params, limits)} {}

Engine::~Engine() {
  _impl = nullptr;
}

Time Engine::minTime() {
  return _impl->minTime();
}

Time Engine::maxTime() {
  return _impl->maxTime();
}

append_result Engine::append(const Meas &value) {
  return _impl->append(value);
}

void Engine::subscribe(const IdArray &ids, const Flag &flag,
                             const ReaderClb_ptr &clbk) {
  _impl->subscribe(ids, flag, clbk);
}

Reader_ptr Engine::currentValue(const IdArray &ids, const Flag &flag) {
  return _impl->currentValue(ids, flag);
}

void Engine::flush() {
  _impl->flush();
}

bool Engine::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                              dariadb::Time *maxResult) {
  return _impl->minMaxTime(id, minResult, maxResult);
}

Cursor_ptr Engine::chunksByIterval(const IdArray &ids, Flag flag,
                                         Time from, Time to) {
  return _impl->chunksByIterval(ids, flag, from, to);
}

IdToChunkMap Engine::chunksBeforeTimePoint(const IdArray &ids, Flag flag,
                                                 Time timePoint) {
  return _impl->chunksBeforeTimePoint(ids, flag, timePoint);
}

IdArray Engine::getIds() {
  return _impl->getIds();
}

size_t Engine::chunks_in_memory() const {
  return _impl->chunks_in_memory();
}

Engine::QueueSizes Engine::queue_size() const {
  return _impl->queue_size();
}
