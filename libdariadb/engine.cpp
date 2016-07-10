#include "engine.h"
#include "flags.h"
#include "storage/bloom_filter.h"
#include "storage/capacitor_manager.h"
#include "storage/inner_readers.h"
#include "storage/lock_manager.h"
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
    raw_res->aof_result = this->aof_result;
    raw_res->local_res = this->local_res;
    return Reader_ptr(raw_res);
  }

  void reset() override {
    TIMECODE_METRICS(ctmd, "read", "UnionReader::reset");
    if (!local_res.empty()) {
      res_it = local_res.begin();
      return;
    }
    // TOOD opt. use Reader::size for alloc.
    /*bool need_sort = false;
    if (!page_result.empty() && !cap_result.empty()) {
      if ((page_result.front().time < cap_result.front().time) &&
          (page_result.back().time < cap_result.front().time)) {
        need_sort = false;
      } else {
        need_sort = true;
      }
    }
    if (need_sort)*/ {
      std::vector<Meas> for_srt(page_result.size() + cap_result.size() +
                                aof_result.size());
      size_t pos = 0;
      for (auto v : page_result) {
        for_srt[pos++] = v;
      }
      for (auto v : cap_result) {
        for_srt[pos++] = v;
      }
      for (auto v : aof_result) {
        for_srt[pos++] = v;
      }

      std::sort(for_srt.begin(), for_srt.end(),
                [](Meas l, Meas r) { return l.time < r.time; });
      local_res = dariadb::Meas::MeasList(for_srt.begin(), for_srt.end());
    } /*else {
      std::copy(std::begin(page_result), std::end(page_result),
                std::back_inserter(local_res));
      std::copy(std::begin(cap_result), std::end(cap_result),
                std::back_inserter(local_res));
    }*/
    page_result.clear();
    cap_result.clear();
    aof_result.clear();

    res_it = local_res.begin();
  }

  size_t size() override { return local_res.size(); }

  dariadb::Meas::MeasList local_res;
  dariadb::Meas::MeasList::const_iterator res_it;
  dariadb::Meas::MeasList page_result;
  dariadb::Meas::MeasList cap_result;
  dariadb::Meas::MeasList aof_result;

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

class AofDropper : public dariadb::storage::AofFileDropper {
  std::string _storage_path;

public:
  AofDropper(std::string storage_path) { _storage_path = storage_path; }
  static void drop(const AOFile_Ptr aof, const std::string &fname,
                   const std::string &storage_path) {
    auto target_name = utils::fs::filename(fname) + CAP_FILE_EXT;
    if (dariadb::utils::fs::path_exists(
            utils::fs::append_path(storage_path, target_name))) {
      return;
    }

    auto ma = aof->readAll();
    CapacitorManager::instance()->append(target_name, ma);
    Manifest::instance()->aof_rm(fname);
    utils::fs::rm(utils::fs::append_path(storage_path, fname));
  }
  void drop(const AOFile_Ptr aof, const std::string fname) override {
    AsyncTask at = [fname, aof, this](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::DROP, ti.kind);
      TIMECODE_METRICS(ctmd, "drop", "AofDropper::drop");
      LockManager::instance()->lock(LockKind::EXCLUSIVE, LockObjects::DROP_AOF);
      AofDropper::drop(aof, fname, _storage_path);
      LockManager::instance()->unlock(LockObjects::DROP_AOF);
    };

    ThreadManager::instance()->post(THREAD_COMMON_KINDS::DROP, AT(at));
  }

  // on start, rm COLA files with name exists AOF file.
  static void cleanStorage(std::string storagePath) {
    auto aofs_lst = utils::fs::ls(storagePath, AOF_FILE_EXT);
    auto caps_lst = utils::fs::ls(storagePath, CAP_FILE_EXT);

    for (auto &aof : aofs_lst) {
      auto aof_fname = utils::fs::filename(aof);
      for (auto &capf : caps_lst) {
        auto cap_fname = utils::fs::filename(capf);
        if (cap_fname == aof_fname) {
          logger_info("fsck: aof drop not finished: " << aof_fname);
          logger_info("fsck: rm " << capf);
          utils::fs::rm(capf);
          Manifest::instance()->cola_rm(utils::fs::extract_filename(capf));
          /* logger_info("open " << aof);
          AOFile::Params aof_param(0,storagePath);

          AOFile_Ptr aof_ptr{ new AOFile(aof_param,aof, true) };
          logger_info("try to drop.");
          AofDropper::drop(aof_ptr, aof, storagePath);
          logger_info("dropped");*/
        }
      }
    }
  }
};

class Engine::Private {
public:
  class CapDrooper : public CapacitorManager::CapDropper {
  public:
    void drop(const std::string &fname) override {
      AsyncTask at = [fname, this](const ThreadInfo &ti) {
        TKIND_CHECK(THREAD_COMMON_KINDS::DROP, ti.kind);
        TIMECODE_METRICS(ctmd, "drop", "CapDrooper::drop");
        // logger_info("cap:drop: begin dropping " << fname);
        LockManager::instance()->lock(LockKind::EXCLUSIVE, LockObjects::DROP_CAP);
        auto p = Capacitor::Params(0, "");
        auto cap = Capacitor_Ptr{new Capacitor{p, fname, false}};

        auto trans = PageManager::instance()->begin_transaction();
        auto cap_header = cap->header();
        cap_header->transaction_number = trans;
        cap->flush();

        cap->drop_to_stor(PageManager::instance());
        cap = nullptr;
        PageManager::instance()->commit_transaction(trans);
        auto without_path = utils::fs::extract_filename(fname);
        Manifest::instance()->cola_rm(without_path);
        utils::fs::rm(fname);
        // logger_info("cap:drop: end.");
        LockManager::instance()->unlock(LockObjects::DROP_CAP);
      };
      ThreadManager::instance()->post(THREAD_COMMON_KINDS::DROP, AT(at));
    }

    static void cleanStorage(std::string storagePath) {
      auto caps_lst = utils::fs::ls(storagePath, CAP_FILE_EXT);
      for (auto c : caps_lst) {
        auto ch = Capacitor::readHeader(c);
        if (ch.transaction_number != 0) {
          auto cap_fname = utils::fs::filename(c);
          logger_info("fsck: rollback #" << ch.transaction_number << " for "
                                         << cap_fname);
          PageManager::instance()->rollback_transaction(ch.transaction_number);
        }
      }
    }
  };
  Private(storage::AOFManager::Params &aof_params,
          const PageManager::Params &page_storage_params,
          dariadb::storage::CapacitorManager::Params &cap_params,
          dariadb::storage::Engine::Limits limits)
      : _page_manager_params(page_storage_params), _cap_params(cap_params),
        _limits(limits) {
    bool is_exists = false;
    if (!dariadb::utils::fs::path_exists(aof_params.path)) {
      dariadb::utils::fs::mkdir(aof_params.path);
    } else {
      is_exists = true;
    }
    _subscribe_notify.start();

    ThreadManager::Params tpm_params(THREAD_MANAGER_COMMON_PARAMS);
    ThreadManager::start(tpm_params);
    LockManager::start(LockManager::Params());
    Manifest::start(utils::fs::append_path(aof_params.path, MANIFEST_FILE_NAME));

    if (is_exists) {
      AofDropper::cleanStorage(aof_params.path);
    }

    PageManager::start(_page_manager_params);
    if (is_exists) {
      CapDrooper::cleanStorage(aof_params.path);
    }
    AOFManager::start(aof_params);
    CapacitorManager::start(_cap_params);
    if (is_exists) {
      CapacitorManager::instance()->fsck();
      PageManager::instance()->fsck();
    }

    _aof_dropper = std::unique_ptr<AofDropper>(new AofDropper(aof_params.path));
    _cap_dropper = std::unique_ptr<CapDrooper>(new CapDrooper());

    AOFManager::instance()->set_downlevel(_aof_dropper.get());
    CapacitorManager::instance()->set_downlevel(_cap_dropper.get());
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
    LockManager::stop();
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
    return std::max(std::max(pmax, cmax), amax);
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult, dariadb::Time *maxResult) {
    TIMECODE_METRICS(ctmd, "minMaxTime", "Engine::minMaxTime");
    std::lock_guard<std::mutex> lg(_locker);
    dariadb::Time subMin1 = dariadb::MAX_TIME, subMax1 = dariadb::MIN_TIME;
    dariadb::Time subMin2 = dariadb::MAX_TIME, subMax2 = dariadb::MIN_TIME;
    dariadb::Time subMin3 = dariadb::MAX_TIME, subMax3 = dariadb::MIN_TIME;
    bool pr, mr, ar;
    pr = mr = ar = false;

    AsyncTask pm_at = [&pr, &subMin1, &subMax1, id](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
      pr = PageManager::instance()->minMaxTime(id, &subMin1, &subMax1);

    };
    AsyncTask cm_at = [&mr, &subMin2, &subMax2, id](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
      mr = CapacitorManager::instance()->minMaxTime(id, &subMin2, &subMax2);
    };
    AsyncTask am_at = [&ar, &subMin3, &subMax3, id](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
      ar = AOFManager::instance()->minMaxTime(id, &subMin3, &subMax3);
    };

    LockManager::instance()->lock(
        LockKind::READ, {LockObjects::PAGE, LockObjects::CAP, LockObjects::AOF});

    auto pm_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::READ, AT(pm_at));
    auto cm_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::READ, AT(cm_at));
    auto am_async = ThreadManager::instance()->post(THREAD_COMMON_KINDS::READ, AT(am_at));

    pm_async->wait();
    cm_async->wait();
    am_async->wait();

    LockManager::instance()->unlock(
        {LockObjects::PAGE, LockObjects::CAP, LockObjects::AOF});

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
    // LockManager::instance()->lock(LockKind::EXCLUSIVE, LockObjects::AOF);
    result = AOFManager::instance()->append(value);
    // LockManager::instance()->unlock(LockObjects::AOF);
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
    LockManager::instance()->lock(LockKind::READ, LockObjects::AOF);
    auto result = AOFManager::instance()->currentValue(ids, flag);
    LockManager::instance()->unlock(LockObjects::AOF);
    return result;
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
    result.active_works= ThreadManager::instance()->active_works();
    return result;
  }

  // Inherited via MeasStorage
  Reader_ptr readInterval(const QueryInterval &q) {
    TIMECODE_METRICS(ctmd, "readInterval", "Engine::readInterval");
    UnionReaderSet *raw_result = new UnionReaderSet();

    Meas::MeasList pm_all;
    Meas::MeasList cap_result;
    Meas::MeasList aof_result;

    AsyncTask pm_at = [&pm_all, &q](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
      // LockManager::instance()->lock(LockKind::READ, LockObjects::PAGE);
      /*logger("engine: pm readinterval...");*/
      auto all_chunkLinks = PageManager::instance()->chunksByIterval(q);

      std::unique_ptr<ChunkReadCallback> callback{new ChunkReadCallback};
      callback->out = &(pm_all);
      PageManager::instance()->readLinks(q, all_chunkLinks, callback.get());
      all_chunkLinks.clear();
      // LockManager::instance()->unlock(LockObjects::PAGE);
      /*logger("engine: pm readinterval end");*/
    };

    AsyncTask cm_at = [&cap_result, &q](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
      /*LockManager::instance()->lock(LockKind::READ, LockObjects::CAP);*/
      /*logger("engine: cm readinterval...");*/
      auto mc_reader = CapacitorManager::instance()->readInterval(q);
      mc_reader->readAll(&cap_result);
      /*LockManager::instance()->unlock(LockObjects::CAP);*/
      /*logger("engine: cm readinterval end");*/
    };

    AsyncTask am_at = [&aof_result, &q](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::READ, ti.kind);
      /*LockManager::instance()->lock(LockKind::READ, LockObjects::AOF);*/
      /*logger("engine: am readinterval...");*/
      auto ardr = AOFManager::instance()->readInterval(q);
      ardr->readAll(&aof_result);
      /*LockManager::instance()->unlock(LockObjects::AOF);*/
      /*logger("engine: am readinterval end");*/
    };

    LockManager::instance()->lock(
        LockKind::READ, {LockObjects::PAGE, LockObjects::CAP, LockObjects::AOF});

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

    LockManager::instance()->unlock(
        {LockObjects::PAGE, LockObjects::CAP, LockObjects::AOF});

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
          raw_res->aof_result.push_back(m);
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

    LockManager::instance()->lock(
        LockKind::READ, {LockObjects::PAGE, LockObjects::CAP, LockObjects::AOF});

    for (auto id : q.ids) {
      dariadb::Time minT, maxT;
      QueryTimePoint local_q = q;
      local_q.ids.clear();
      local_q.ids.push_back(id);

      if (AOFManager::instance()->minMaxTime(id, &minT, &maxT) &&
          (minT < q.time_point || maxT < q.time_point)) {
        auto subres = AOFManager::instance()->readInTimePoint(local_q);
        raw_result->add_rdr(subres);
      } else {
        if (CapacitorManager::instance()->minMaxTime(id, &minT, &maxT) &&
            (utils::inInterval(minT, maxT, q.time_point))) {
          auto subres = CapacitorManager::instance()->readInTimePoint(local_q);
          raw_result->add_rdr(subres);
        } else {
          auto id2meas = PageManager::instance()->valuesBeforeTimePoint(local_q);

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
    }
    LockManager::instance()->unlock(
        {LockObjects::PAGE, LockObjects::CAP, LockObjects::AOF});
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

  void drop_part_caps(size_t count) { CapacitorManager::instance()->drop_part(count); }

protected:
  storage::PageManager::Params _page_manager_params;
  dariadb::storage::CapacitorManager::Params _cap_params;
  dariadb::storage::Engine::Limits _limits;

  mutable std::mutex _locker;
  SubscribeNotificator _subscribe_notify;
  Id _next_query_id;
  std::unordered_map<Id, std::shared_ptr<Meas::MeasList>> _load_results;
  std::unique_ptr<AofDropper> _aof_dropper;
  std::unique_ptr<CapDrooper> _cap_dropper;
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

void Engine::drop_part_caps(size_t count) {
  return _impl->drop_part_caps(count);
}
