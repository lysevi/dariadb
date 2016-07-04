#include "engine.h"
#include "flags.h"
#include "storage/bloom_filter.h"
#include "storage/capacitor_manager.h"
#include "storage/inner_readers.h"
#include "storage/manifest.h"
#include "storage/page_manager.h"
#include "storage/subscribe.h"
#include "utils/exception.h"
#include "utils/locker.h"
#include "utils/logger.h"
#include "utils/metrics.h"
#include "utils/thread_manager.h"
#include <algorithm>
#include <cassert>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils::async;

class UnionReader : public Reader {
public:
  UnionReader(dariadb::Flag flag, dariadb::Time from, dariadb::Time to)
      : _flag(flag), _from(from), _to(to) {}
  // Inherited via Reader
  bool isEnd() const override { return local_res.empty() || res_it == local_res.end(); }

  IdArray getIds() const override { return _ids; }

  void readNext(ReaderClb *clb) override {
    if (isEnd()) {
      return;
    }
    clb->call(*res_it);
    ++res_it;
  }

  Reader_ptr clone() const override {
    UnionReader *raw_res = new UnionReader(_flag, _from, _to);
    raw_res->_ids = this->_ids;
    raw_res->page_result = this->page_result;
    raw_res->cap_result = this->cap_result;
    raw_res->local_res = this->local_res;
    return Reader_ptr(raw_res);
  }

  void reset() override {
    TIMECODE_METRICS(ctmd, "read", "UnionReader::reset");
    // TOOD opt. use Reader::size for alloc.
    bool need_sort = false;
    if (!page_result.empty() && !cap_result.empty()) {
      if ((page_result.front().time < cap_result.front().time) &&
          (page_result.back().time < cap_result.front().time)) {
        need_sort = false;
      } else {
        need_sort = true;
      }
    }
    if (need_sort) {
      std::vector<Meas> for_srt(page_result.size() + cap_result.size());
      size_t pos = 0;
      for (auto v : page_result) {
        for_srt[pos++] = v;
      }
      for (auto v : cap_result) {
        for_srt[pos++] = v;
      }

      std::sort(for_srt.begin(), for_srt.end(),
                [](Meas l, Meas r) { return l.time < r.time; });
      local_res = dariadb::Meas::MeasList(for_srt.begin(), for_srt.end());
    } else {
      std::copy(std::begin(page_result), std::end(page_result),
                std::back_inserter(local_res));
      std::copy(std::begin(cap_result), std::end(cap_result),
                std::back_inserter(local_res));
    }
    page_result.clear();
    cap_result.clear();

    res_it = local_res.begin();
  }

  size_t size() override { return local_res.size(); }

  dariadb::Meas::MeasList local_res;
  dariadb::Meas::MeasList::const_iterator res_it;
  dariadb::Meas::MeasList page_result;
  dariadb::Meas::MeasList cap_result;

  dariadb::Flag _flag;
  dariadb::Time _from;
  dariadb::Time _to;
  dariadb::IdArray _ids;
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
    class AofDropper : public dariadb::storage::AofFileDropper {
		std::string _storage_path;
    public:
		AofDropper(std::string storage_path) {
			_storage_path = storage_path;
		}
      void drop(std::string filename,const dariadb::Meas::MeasArray&ma) override {
		  AsyncTask at = [filename,ma, this](const ThreadInfo&ti) {
			  TKIND_CHECK(THREAD_COMMON_KINDS::CAP_DROP, ti.kind);
			  auto target_name = filename + CAP_FILE_EXT;
			  if (dariadb::utils::fs::path_exists(utils::fs::append_path(_storage_path, target_name))) {
				  return;
			  }
			  CapacitorManager::instance()->append(target_name, ma);
			  Manifest::instance()->aof_rm(filename);
			  utils::fs::rm(utils::fs::append_path(_storage_path, filename));
		  };

		  ThreadManager::instance()->post(THREAD_COMMON_KINDS::CAP_DROP, AT(at));
      }
    };

  Private(storage::AOFManager::Params &aof_params,
          const PageManager::Params &page_storage_params,
          dariadb::storage::CapacitorManager::Params &cap_params,
          dariadb::storage::Engine::Limits limits)
      : _page_manager_params(page_storage_params), _cap_params(cap_params),
        _limits(limits) {
	  if (!dariadb::utils::fs::path_exists(aof_params.path)) {
		  dariadb::utils::fs::mkdir(aof_params.path);
	  }
    _subscribe_notify.start();
	
	ThreadManager::Params tpm_params(THREAD_MANAGER_COMMON_PARAMS);
	ThreadManager::start(tpm_params);

	Manifest::start(utils::fs::append_path(aof_params.path, MANIFEST_FILE_NAME));

    PageManager::start(_page_manager_params);
    AOFManager::start(aof_params);
    CapacitorManager::start(_cap_params);
    _aof_dropper=std::unique_ptr<AofDropper>(new AofDropper(aof_params.path));
    auto pm = PageManager::instance();
    AOFManager::instance()->set_downlevel(_aof_dropper.get());
    CapacitorManager::instance()->set_downlevel(pm);
    _next_query_id = Id();
  }
  ~Private() {
    _subscribe_notify.stop();
    this->flush();

    AOFManager::stop();
    CapacitorManager::stop();
    PageManager::stop();
	Manifest::stop();
	ThreadManager::stop();
  }

  Time minTime() {
    std::lock_guard<std::mutex> lg(_locker);
    auto pmin = PageManager::instance()->minTime();
    auto cmin = CapacitorManager::instance()->minTime();
    auto amin = AOFManager::instance()->minTime();
    return std::min(std::min(pmin, cmin), amin);
  }

  Time maxTime() {
    std::lock_guard<std::mutex> lg(_locker);
    auto pmax = PageManager::instance()->maxTime();
    auto cmax = CapacitorManager::instance()->maxTime();
    auto amax = AOFManager::instance()->maxTime();
    return std::max(std::min(pmax, cmax), amax);
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult, dariadb::Time *maxResult) {
    TIMECODE_METRICS(ctmd, "minMaxTime", "Engine::minMaxTime");
    std::lock_guard<std::mutex> lg(_locker);
	dariadb::Time subMin1 = dariadb::MAX_TIME, subMax1 = dariadb::MIN_TIME;
	dariadb::Time subMin2 = dariadb::MAX_TIME, subMax2 = dariadb::MIN_TIME;
	dariadb::Time subMin3 = dariadb::MAX_TIME, subMax3 = dariadb::MIN_TIME;
	bool pr, mr, ar;
	pr = mr = ar = false;
	AsyncTask  pm_at = [&pr, &subMin1, &subMax1, id](const ThreadInfo&ti) {
		TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
		pr = PageManager::instance()->minMaxTime(id, &subMin1, &subMax1);
	};
	AsyncTask cm_at = [&mr, &subMin2, &subMax2, id](const ThreadInfo&ti) {
		TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
		mr = CapacitorManager::instance()->minMaxTime(id, &subMin2, &subMax2);
	};
	AsyncTask am_at = [&ar, &subMin3, &subMax3, id](const ThreadInfo&ti) {
		TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
		ar = AOFManager::instance()->minMaxTime(id, &subMin3, &subMax3);
	};

    auto pm_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::READ, AT(pm_at));
    auto cm_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::READ, AT(cm_at));
    auto am_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::READ, AT(am_at));

	pm_async->wait();
	cm_async->wait();
	am_async->wait();

    *minResult = dariadb::MAX_TIME;
    *maxResult = dariadb::MIN_TIME;

    *minResult = std::min(subMin1, subMin2);
    *minResult = std::min(*minResult, subMin3);
    *maxResult = std::max(subMax1, subMax2);
    *maxResult = std::max(*maxResult, subMax3);
    return pr || mr || ar;
  }

  append_result append(const Meas &value) {
    append_result result{};
    result = AOFManager::instance()->append(value);
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
    return AOFManager::instance()->currentValue(ids, flag);
  }

  void flush() {
    TIMECODE_METRICS(ctmd, "flush", "Engine::flush");
    std::lock_guard<std::mutex> lg(_locker);
    AOFManager::instance()->flush();
    CapacitorManager::instance()->flush();
    PageManager::instance()->flush();
	ThreadManager::instance()->flush();
  }

  class ChunkReadCallback : public ReaderClb {
  public:
    virtual void call(const Meas &m) override { out->push_back(m); }
    Meas::MeasList *out;
  };

  Engine::QueueSizes queue_size() const {
    QueueSizes result;
    result.aofs_count = AOFManager::instance()->files_count();
    result.pages_count = PageManager::instance()->files_count();
    result.cola_count = CapacitorManager::instance()->files_count();
    return result;
  }

  // Inherited via MeasStorage
  Reader_ptr readInterval(const QueryInterval &q) {
    TIMECODE_METRICS(ctmd, "readInterval", "Engine::readInterval");
    UnionReaderSet *raw_result = new UnionReaderSet();

	Meas::MeasList pm_all;
	Meas::MeasList cap_result;
	Meas::MeasList aof_result;

	AsyncTask pm_at= [&pm_all, &q](const ThreadInfo&ti) {
		TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
        /*logger("engine: pm readinterval...");*/
		auto all_chunkLinks = PageManager::instance()->chunksByIterval(q);

		std::unique_ptr<ChunkReadCallback> callback{ new ChunkReadCallback };
		callback->out = &(pm_all);
		PageManager::instance()->readLinks(q, all_chunkLinks, callback.get());
		all_chunkLinks.clear();
        /*logger("engine: pm readinterval end");*/
	};

	AsyncTask cm_at= [&cap_result, &q](const ThreadInfo&ti) {
		TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
        /*logger("engine: cm readinterval...");*/
		auto mc_reader = CapacitorManager::instance()->readInterval(q);
		mc_reader->readAll(&cap_result);
        /*logger("engine: cm readinterval end");*/
	};

	AsyncTask am_at = [&aof_result, &q](const ThreadInfo&ti) {
		TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
        /*logger("engine: am readinterval...");*/
		auto ardr = AOFManager::instance()->readInterval(q);
		ardr->readAll(&aof_result);
        /*logger("engine: am readinterval end");*/
	};
	

    auto pm_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::READ, AT(pm_at));
    auto cm_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::READ, AT(cm_at));
    auto am_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::READ, AT(am_at));

    /*logger("engine: interval: wait pm.");*/
	pm_async->wait();
    /*logger("engine: interval: wait cm.");*/
    cm_async->wait();
    /*logger("engine: interval: wait am.");*/
	am_async->wait();
    /*logger("engine: interval: wait all and.");*/
    for (auto id : q.ids) {
      UnionReader *raw_res = new UnionReader(q.flag, q.from, q.to);
      raw_res->_ids.resize(1);
      raw_res->_ids[0] = id;

      for (auto &m : pm_all) {
        if (m.id == id) {
          raw_res->page_result.push_back(m);
        }
      }

      pm_all.erase(std::remove_if(pm_all.begin(), pm_all.end(),
                                  [id](const Meas &m) { return m.id == id; }),
                   pm_all.end());
      for (auto &m : cap_result) {
        if (m.id == id) {
          raw_res->cap_result.push_back(m);
        }
      }

	  cap_result.erase(std::remove_if(cap_result.begin(), cap_result.end(),
		  [id](const Meas &m) { return m.id == id; }),
		  cap_result.end());

      for (auto &m : aof_result) {
        if (m.id == id) {
          raw_res->cap_result.push_back(m);
        }
	  }
	  aof_result.erase(std::remove_if(aof_result.begin(), aof_result.end(),
		  [id](const Meas &m) { return m.id == id; }),
		  aof_result.end());

      raw_result->add_rdr(Reader_ptr{raw_res});
    }
    raw_result->reset();
    /*logger("engine: interval end.");*/
    return Reader_ptr(raw_result);
  }

  Reader_ptr readInTimePoint(const QueryTimePoint &q) {
    TIMECODE_METRICS(ctmd, "readInTimePoint", "Engine::readInTimePoint");
    UnionReaderSet *raw_result = new UnionReaderSet();

	/*auto pm_id2meas = PageManager::instance()->valuesBeforeTimePoint(q);
	Meas::MeasList aof_values;
	AOFManager::instance()->readInTimePoint(q)->readAll(&aof_values);
	Meas::MeasList cap_values;
	CapacitorManager::instance()->readInTimePoint(q)->readAll(&cap_values);

	for (auto id : q.ids) {
		TP_Reader *raw_tp_reader = new TP_Reader;
		raw_tp_reader->_ids.resize(size_t(1));
		raw_tp_reader->_ids[0] = id;

		Meas result_value=pm_id2meas[id];
		for (auto&m : aof_values) {
			if (m.id == id) {
				if (m.flag != Flags::_NO_DATA && m.time > result_value.time) {
					result_value = m;
				}
				break;
			}
		}
		
		for (auto&m : cap_values) {
			if (m.id == id) {
				if (m.flag != Flags::_NO_DATA && m.time > result_value.time) {
					result_value = m;
				}
				break;
			}
		}
		raw_tp_reader->_values.push_back(result_value);
		raw_tp_reader->reset();
		Reader_ptr subres{ raw_tp_reader };
		raw_result->add_rdr(subres);

	}*/
	for (auto id : q.ids) {
		dariadb::Time minT, maxT;
		QueryTimePoint local_q = q;
		local_q.ids.clear();
		local_q.ids.push_back(id);

		if (AOFManager::instance()->minMaxTime(id, &minT, &maxT) &&
			(minT < q.time_point || maxT < q.time_point)) {
			auto subres = AOFManager::instance()->readInTimePoint(local_q);
			raw_result->add_rdr(subres);
		}
		else {
			if (CapacitorManager::instance()->minMaxTime(id, &minT, &maxT) &&
				(utils::inInterval(minT, maxT, q.time_point))) {
				auto subres = CapacitorManager::instance()->readInTimePoint(local_q);
				raw_result->add_rdr(subres);
			}
			else {
				auto id2meas = PageManager::instance()->valuesBeforeTimePoint(local_q);

				TP_Reader *raw_tp_reader = new TP_Reader;
				raw_tp_reader->_ids.resize(size_t(1));
				raw_tp_reader->_ids[0] = id;
				auto fres = id2meas.find(id);

				if (fres != id2meas.end()) {
					raw_tp_reader->_values.push_back(fres->second);
				}
				else {
					if (id2meas.empty()) {
						auto e = Meas::empty(id);
						e.flag = Flags::_NO_DATA;
						e.time = q.time_point;
						raw_tp_reader->_values.push_back(e);
					}
				}
				raw_tp_reader->reset();
				Reader_ptr subres{ raw_tp_reader };
				raw_result->add_rdr(subres);
			}
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
  storage::PageManager::Params _page_manager_params;
  dariadb::storage::CapacitorManager::Params _cap_params;
  dariadb::storage::Engine::Limits _limits;

  mutable std::mutex _locker;
  SubscribeNotificator _subscribe_notify;
  Id _next_query_id;
  std::unordered_map<Id, std::shared_ptr<Meas::MeasList>> _load_results;
  std::unique_ptr<AofDropper> _aof_dropper;
};

Engine::Engine(storage::AOFManager::Params aof_params,
               storage::PageManager::Params page_manager_params,
               dariadb::storage::CapacitorManager::Params cap_params,
               const dariadb::storage::Engine::Limits &limits)
    : _impl{new Engine::Private(aof_params, page_manager_params, cap_params, limits)} {}

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

Reader_ptr Engine::readInterval(const QueryInterval &q) {
  return _impl->readInterval(q);
}

Reader_ptr Engine::readInTimePoint(const QueryTimePoint &q) {
  return _impl->readInTimePoint(q);
}
