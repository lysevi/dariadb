#include "engine.h"
#include "flags.h"
#include "storage/bloom_filter.h"
#include "storage/capacitor.h"
#include "storage/inner_readers.h"
#include "storage/page_manager.h"
#include "storage/subscribe.h"
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
  bool isEnd() const override { return local_res.empty() || res_it == local_res.end(); }

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
    if (isEnd()) {
      return;
    }
    clb->call(*res_it);
    ++res_it;
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
    local_res.clear();
    // TOOD opt. use Reader::size for alloc.
    dariadb::Meas::MeasList tmp_p;
    if (page_reader != nullptr) {
      page_reader->reset();
      page_reader->readAll(&tmp_p);
    }
    dariadb::Meas::MeasList tmp_c;
    if (cap_reader != nullptr) {
      cap_reader->reset();
      cap_reader->readAll(&tmp_c);
    }

    bool need_sort = false;
    if (!tmp_p.empty() && !tmp_c.empty()) {
      if ((tmp_p.front().time < tmp_c.front().time) &&
          (tmp_p.back().time < tmp_c.front().time)) {
        need_sort = false;
      } else {
        need_sort = true;
      }
    }
    if (need_sort) {
      std::vector<Meas> for_srt(tmp_p.size() + tmp_c.size());
      size_t pos = 0;
      for (auto v : tmp_p) {
        for_srt[pos++] = v;
      }
      for (auto v : tmp_c) {
        for_srt[pos++] = v;
      }

      std::sort(for_srt.begin(), for_srt.end(),
                [](Meas l, Meas r) { return l.time < r.time; });
      local_res = dariadb::Meas::MeasList(for_srt.begin(), for_srt.end());
    } else {
      std::copy(std::begin(tmp_p), std::end(tmp_p), std::back_inserter(local_res));
      std::copy(std::begin(tmp_c), std::end(tmp_c), std::back_inserter(local_res));
    }

    res_it = local_res.begin();
  }
  size_t size() { return local_res.size(); }
  dariadb::Meas::MeasList local_res;
  dariadb::Meas::MeasList::const_iterator res_it;
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

  void add_rdr(const Reader_ptr &cptr) { _readers.push_back(cptr); }

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
  size_t size() override {
    size_t result = 0;
    for (auto &c : _readers) {
      result += c->size();
    }
    return result;
  }
};

class Engine::Private {
public:
  Private(const PageManager::Params &page_storage_params,
          dariadb::storage::Capacitor::Params cap_params,
          dariadb::storage::Engine::Limits limits)
      : _page_manager_params(page_storage_params), _cap_params(cap_params),
        _limits(limits) {
    _subscribe_notify.start();

    PageManager::start(_page_manager_params);

    mem_cap = new Capacitor(PageManager::instance(), _cap_params);
    _next_query_id = Id();
  }
  ~Private() {
    _subscribe_notify.stop();
    this->flush();
    delete mem_cap;
    PageManager::stop();
  }

  Time minTime() {
    std::lock_guard<std::mutex> lg(_locker);
    auto pmin = PageManager::instance()->minTime();
    auto cmin = mem_cap->minTime();
    return std::min(pmin, cmin);
  }

  Time maxTime() {
    std::lock_guard<std::mutex> lg(_locker);
    auto pmax = PageManager::instance()->maxTime();
    auto cmax = mem_cap->maxTime();
    return std::max(pmax, cmax);
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult, dariadb::Time *maxResult) {
    std::lock_guard<std::mutex> lg(_locker);
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
    std::lock_guard<std::mutex> lg(_locker);
    this->mem_cap->flush();
    PageManager::instance()->flush();
  }

  class ChunkReadCallback : public ReaderClb {
  public:
    virtual void call(const Meas &m) override { out->push_back(m); }
    Meas::MeasList *out;
  };

  Engine::QueueSizes queue_size() const {
    QueueSizes result;
    result.pages_count = PageManager::instance()->files_count();
    result.cola_count = this->mem_cap->files_count();
    return result;
  }

  // Inherited via MeasStorage
  Reader_ptr readInterval(const QueryInterval &q) {
    UnionReaderSet *raw_result = new UnionReaderSet();

    auto all_chunkLinks = PageManager::instance()->chunksByIterval(q);

    for (auto id : q.ids) {

      InnerReader *page_rdr = new InnerReader(q.flag, q.from, q.to);
      UnionReader *raw_res = new UnionReader();
      raw_res->page_reader = Reader_ptr{page_rdr};
      page_rdr->_ids.push_back(id);

      dariadb::Time minT, maxT;
      QueryInterval local_q = q;
      local_q.ids.clear();
      local_q.ids.push_back(id);
      if (!mem_cap->minMaxTime(id, &minT, &maxT)) {
        ChunkLinkList chunks_for_id;
        for (auto &c : all_chunkLinks) {
          if (c.id_bloom == id) {
            chunks_for_id.push_back(c);
          }
        }
        
        std::unique_ptr<ChunkReadCallback> callback{new ChunkReadCallback};
        callback->out =&(page_rdr->_values);
        PageManager::instance()->readLinks(local_q, chunks_for_id, callback.get());
        
      } else {

        if (minT <= q.from && maxT >= q.to) {
          auto mc_reader = mem_cap->readInterval(local_q);
          raw_res->cap_reader = Reader_ptr{mc_reader};
        } else {
          ChunkLinkList chunks_for_id;
          for (auto &c : all_chunkLinks) {
            if (c.id_bloom == id) {
              chunks_for_id.push_back(c);
            }
          }
          
          std::unique_ptr<ChunkReadCallback> callback{new ChunkReadCallback};
          callback->out = &(page_rdr->_values);
          PageManager::instance()->readLinks(local_q, chunks_for_id, callback.get());
          
          local_q.from = minT;
          local_q.to = q.to;

          auto mc_reader = mem_cap->readInterval(local_q);
          raw_res->cap_reader = Reader_ptr{mc_reader};
        }
      }
      raw_result->add_rdr(Reader_ptr{raw_res});
    }
    raw_result->reset();
    return Reader_ptr(raw_result);
  }

  Reader_ptr readInTimePoint(const QueryTimePoint &q) {
    UnionReaderSet *raw_result = new UnionReaderSet();
    auto id2meas = PageManager::instance()->valuesBeforeTimePoint(q);

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
        TP_Reader *raw_tp_reader = new TP_Reader;
        raw_tp_reader->_ids.resize(size_t(1));
        raw_tp_reader->_ids[0] = id;
        auto fres = id2meas.find(id);
        if (fres != id2meas.end()) {
          raw_tp_reader->_values.push_back(fres->second);
        } else {
          if (id2meas.empty()) {
            auto e = Meas::empty(id);
            e.flag = Flags::_NO_DATA;
            e.time = q.time_point;
            raw_tp_reader->_values.push_back(e);
          }
        }
        raw_tp_reader->reset();
        Reader_ptr subres{raw_tp_reader};
        raw_result->add_rdr(subres);
      }
    }
    raw_result->reset();
    return Reader_ptr(raw_result);
  }

  Id load(const QueryInterval &qi) {
    std::lock_guard<std::mutex> lg(_locker);
    Id result = _next_query_id++;

    auto reader = this->readInterval(qi);
    std::shared_ptr<Meas::MeasList> reader_values = std::make_shared<Meas::MeasList>();
    reader->readAll(reader_values.get());
    _load_results[result] = reader_values;
    return result;
  }

  Id load(const QueryTimePoint &qt) {
    std::lock_guard<std::mutex> lg(_locker);
    Id result = _next_query_id++;

    auto reader = this->readInTimePoint(qt);
    std::shared_ptr<Meas::MeasList> reader_values = std::make_shared<Meas::MeasList>();
    reader->readAll(reader_values.get());
    _load_results[result] = reader_values;
    return result;
  }

  Meas::MeasList getResult(Id id) {
    // TODO  return ref/ptr.
    Meas::MeasList result;
    auto fres = _load_results.find(id);
    if (fres == _load_results.end()) {
      return result;
    } else {
      for (auto &v : *(fres->second)) {
        result.push_back(v);
      }
      return result;
    }
  }

protected:
  storage::Capacitor *mem_cap;

  storage::PageManager::Params _page_manager_params;
  dariadb::storage::Capacitor::Params _cap_params;
  dariadb::storage::Engine::Limits _limits;

  mutable std::mutex _locker;
  SubscribeNotificator _subscribe_notify;
  Id _next_query_id;
  std::unordered_map<Id, std::shared_ptr<Meas::MeasList>> _load_results;
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

Id dariadb::storage::Engine::load(const QueryInterval &qi) {
  return _impl->load(qi);
}

Id dariadb::storage::Engine::load(const QueryTimePoint &qt) {
  return _impl->load(qt);
}

Meas::MeasList dariadb::storage::Engine::getResult(Id id) {
  return _impl->getResult(id);
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
