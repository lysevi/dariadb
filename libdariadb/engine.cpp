#include "engine.h"
#include "flags.h"
#include "storage/capacitor.h"
#include "storage/inner_readers.h"
#include "storage/page_manager.h"
#include "storage/subscribe.h"
#include "storage/bloom_filter.h"
#include "utils/exception.h"
#include "utils/locker.h"
#include <algorithm>
#include <cassert>

using namespace dariadb;
using namespace dariadb::storage;

class UnionReader : public Reader {
public:
  UnionReader() { cap_reader = page_reader = nullptr; }
  // Inherited via Reader
  bool isEnd() const override {
    return (page_reader == nullptr || page_reader->isEnd()) &&
           (cap_reader == nullptr || cap_reader->isEnd());
  }

  IdArray getIds() const override {
    dariadb::IdSet idset;
    if (page_reader != nullptr) {
      auto sub_res = page_reader->getIds();
      for (auto id : sub_res) {
        idset.insert(id);
      }
    }
    {
      if (cap_reader != nullptr) {
        auto sub_res = cap_reader->getIds();
        for (auto id : sub_res) {
          idset.insert(id);
        }
      }
    }
    return dariadb::IdArray{idset.begin(), idset.end()};
  }

  void readNext(ReaderClb *clb) override {
    if (page_reader != nullptr && !page_reader->isEnd()) {
      page_reader->readNext(clb);
    } else {
      if (cap_reader != nullptr && !cap_reader->isEnd()) {
        cap_reader->readNext(clb);
      }
    }
  }
  Reader_ptr clone() const override {
    UnionReader *raw_res = new UnionReader();
    if (this->page_reader != nullptr) {
      raw_res->page_reader = this->page_reader->clone();
    }
    if (this->cap_reader != nullptr) {
      raw_res->cap_reader = this->cap_reader->clone();
    }
    return Reader_ptr(raw_res);
  }

  void reset() override {
    if (page_reader != nullptr) {
      page_reader->reset();
    }
    if (cap_reader != nullptr) {
      cap_reader->reset();
    }
  }

  Reader_ptr page_reader;
  Reader_ptr cap_reader;
};

class UnionReaderSet : public Reader {
public:
  std::list<Reader_ptr> _readers;
  std::list<Reader_ptr>::iterator it;
  bool _is_end;
  UnionReaderSet() {}
  ~UnionReaderSet() { _readers.clear(); }

  void add_rdr(const Reader_ptr &cptr) {
    _readers.push_back(cptr);
    reset();
  }

  IdArray getIds() const override {
    IdSet subresult;
    for (auto r : _readers) {
      IdArray ids = r->getIds();
      for (auto id : ids) {
        subresult.insert(id);
      }
    }
    return IdArray{subresult.begin(), subresult.end()};
  }

  bool isEnd() const override { return _is_end; }

  void readNext(ReaderClb *clb) override {
    (*it)->readNext(clb);
    if ((*it)->isEnd()) {
      ++it;
      if (it == _readers.end()) {
        _is_end = true;
      }
    }
  }

  Reader_ptr clone() const override {
    UnionReaderSet *res = new UnionReaderSet;
    for (auto r : _readers) {
      res->add_rdr(r->clone());
    }
    return Reader_ptr{res};
  }
  void reset() override {
    it = _readers.begin();
    _is_end = false;
    for (auto &c : _readers) {
      c->reset();
    }
  }
};

class Engine::Private {
public:
  Private(const PageManager::Params &page_storage_params,
          dariadb::storage::Capacitor::Params cap_params,
          dariadb::storage::Engine::Limits limits)
      : /*mem_storage{new MemoryStorage()},*/ _page_manager_params(page_storage_params),
        _cap_params(cap_params), _limits(limits) {
    _subscribe_notify.start();

    PageManager::start(_page_manager_params);

    mem_cap = new Capacitor(PageManager::instance(), _cap_params);
    // mem_storage_raw = dynamic_cast<MemoryStorage *>(mem_storage.get());
  }
  ~Private() {
    _subscribe_notify.stop();
    this->flush();
    delete mem_cap;
    PageManager::stop();
  }

  Time minTime() {
    std::lock_guard<std::recursive_mutex> lg(_locker);
    auto pmin = PageManager::instance()->minTime();
    auto cmin = mem_cap->minTime();
    return std::min(pmin, cmin);
  }

  Time maxTime() {
    std::lock_guard<std::recursive_mutex> lg(_locker);
    auto pmax = PageManager::instance()->maxTime();
    auto cmax = mem_cap->maxTime();
    return std::max(pmax, cmax);
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult, dariadb::Time *maxResult) {
    std::lock_guard<std::recursive_mutex> lg(_locker);
    dariadb::Time subMin1, subMax1;
    auto pr = PageManager::instance()->minMaxTime(id, &subMin1, &subMax1);
    dariadb::Time subMin2, subMax2;
    auto mr = mem_cap->minMaxTime(id, &subMin2, &subMax2);

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

  append_result append(const Meas &value) {
    append_result result{};
    // result = PageManager::instance()->append(value);
    result = mem_cap->append(value);
    if (result.writed == 1) {
      _subscribe_notify.on_append(value);
    }

    return result;
  }

  void subscribe(const IdArray &ids, const Flag &flag, const ReaderClb_ptr &clbk) {
    auto new_s = std::make_shared<SubscribeInfo>(ids, flag, clbk);
    _subscribe_notify.add(new_s);
  }

  Reader_ptr currentValue(const IdArray &ids, const Flag &flag) {
    return mem_cap->currentValue(ids, flag);
  }

  void flush() {
    std::lock_guard<std::recursive_mutex> lg(_locker);
    this->mem_cap->flush();
    PageManager::instance()->flush();
  }

  Engine::QueueSizes queue_size() const {
    QueueSizes result;
    result.page = PageManager::instance()->in_queue_size();
    result.cap = this->mem_cap->in_queue_size();
    return result;
  }

 
  // Inherited via MeasStorage
  Reader_ptr readInterval(const QueryInterval &q) {
    UnionReaderSet *raw_result = new UnionReaderSet();

	auto tmp_chunkLinks = PageManager::instance()->chunksByIterval(q);
	auto tmp_page_cursor = PageManager::instance()->readLinks(tmp_chunkLinks);
	ChunksList all_chunks_lst;
	tmp_page_cursor->readAll(&all_chunks_lst);
	tmp_page_cursor = nullptr;

    for (auto id : q.ids) {
      InnerReader *raw_rdr = new InnerReader(q.flag, q.from, q.to);
      UnionReader *raw_res = new UnionReader();
      raw_res->page_reader = Reader_ptr{raw_rdr};
      raw_rdr->_ids.push_back(id);

      dariadb::Time minT, maxT;
      QueryInterval local_q = q;
      local_q.ids.clear();
      local_q.ids.push_back(id);
      if (!mem_cap->minMaxTime(id, &minT, &maxT)) {
		  ChunkCursor* page_cursor_raw = new ChunkCursor;
		  for (auto &c : all_chunks_lst) {
			  if (bloom_check(c->info->id_bloom, id)) {
				  page_cursor_raw->chunks.push_back(c);
			  }
		  }
		  raw_rdr->add(Cursor_ptr{ page_cursor_raw });
      } else {

        if (minT <= q.from && maxT >= q.to) {
          auto mc_reader = mem_cap->readInterval(local_q);
          raw_res->cap_reader = Reader_ptr{mc_reader};
        } else {
          local_q.to = minT;
          auto chunkLinks = PageManager::instance()->chunksByIterval(local_q);
		  ChunkCursor* page_cursor_raw = new ChunkCursor;
		  for (auto &c : all_chunks_lst) {
			  if (bloom_check(c->info->id_bloom, id) && c->info->minTime<=local_q.to) {
				  page_cursor_raw->chunks.push_back(c);
			  }
		  }
		  raw_rdr->add(Cursor_ptr{ page_cursor_raw });
          local_q.from = minT;
          local_q.to = q.to;

          auto mc_reader = mem_cap->readInterval(local_q);
          raw_res->cap_reader = Reader_ptr{mc_reader};
        }
      }
      raw_result->add_rdr(Reader_ptr{raw_res});
    }
    return Reader_ptr(raw_result);
  }

  Reader_ptr readInTimePoint(const QueryTimePoint &q) {
    UnionReaderSet *raw_result = new UnionReaderSet();
    for (auto id : q.ids) {
      dariadb::Time minT, maxT;
      QueryTimePoint local_q = q;
      local_q.ids.clear();
      local_q.ids.push_back(id);

      if (mem_cap->minMaxTime(id, &minT, &maxT) &&
          utils::inInterval(minT, maxT, local_q.time_point)) {
        auto subres = mem_cap->readInTimePoint(local_q);
        raw_result->add_rdr(subres);
      } else {
        auto id2meas = PageManager::instance()->valuesBeforeTimePoint(local_q);

        TP_Reader *raw_tp_reader = new TP_Reader;
        if (!id2meas.empty()) {
          raw_tp_reader->_ids.resize(id2meas.size());
          for (auto kv : id2meas) {
            raw_tp_reader->_values.push_back(kv.second);
            raw_tp_reader->_ids.push_back(kv.first);
          }
        } else {
          if (id2meas.empty()) {
            raw_tp_reader->_ids.resize(size_t(1));
            auto e = Meas::empty(id);
            e.flag = Flags::_NO_DATA;
            e.time = q.time_point;
            raw_tp_reader->_values.push_back(e);
            raw_tp_reader->_ids.push_back(id);
          }
        }
        raw_tp_reader->reset();
        Reader_ptr subres{raw_tp_reader};
        raw_result->add_rdr(subres);
      }
    }
    return Reader_ptr(raw_result);
  }

protected:
  //  std::shared_ptr<MemoryStorage> mem_storage;
  // storage::MemoryStorage *mem_storage_raw;
  storage::Capacitor *mem_cap;

  storage::PageManager::Params _page_manager_params;
  dariadb::storage::Capacitor::Params _cap_params;
  dariadb::storage::Engine::Limits _limits;

  mutable std::recursive_mutex _locker;
  SubscribeNotificator _subscribe_notify;
};

Engine::Engine(storage::PageManager::Params page_manager_params,
               dariadb::storage::Capacitor::Params cap_params,
               const dariadb::storage::Engine::Limits &limits)
    : _impl{new Engine::Private(page_manager_params, cap_params, limits)} {}

Engine::~Engine() {
  _impl = nullptr;
}

Time Engine::minTime() {
  return _impl->minTime();
}

Time Engine::maxTime() {
  return _impl->maxTime();
}
bool Engine::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                        dariadb::Time *maxResult) {
  return _impl->minMaxTime(id, minResult, maxResult);
}

append_result Engine::append(const Meas &value) {
  return _impl->append(value);
}

void Engine::subscribe(const IdArray &ids, const Flag &flag, const ReaderClb_ptr &clbk) {
  _impl->subscribe(ids, flag, clbk);
}

Reader_ptr Engine::currentValue(const IdArray &ids, const Flag &flag) {
  return _impl->currentValue(ids, flag);
}

void Engine::flush() {
  _impl->flush();
}

Engine::QueueSizes Engine::queue_size() const {
  return _impl->queue_size();
}

// Reader_ptr dariadb::storage::Engine::readInterval(Time from, Time to){
//	return _impl->readInterval(from, to);
//}

// Reader_ptr dariadb::storage::Engine::readInTimePoint(Time time_point) {
//	return _impl->readInTimePoint(time_point);
//}

Reader_ptr Engine::readInterval(const QueryInterval &q) {
  return _impl->readInterval(q);
}

Reader_ptr Engine::readInTimePoint(const QueryTimePoint &q) {
  return _impl->readInTimePoint(q);
}
