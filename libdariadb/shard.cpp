#include <extern/json/src/json.hpp>

#include <libdariadb/engine.h>
#include <libdariadb/shard.h>
#include <libdariadb/utils/async/locker.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <libdariadb/utils/fs.h>

#include <fstream>

const std::string SHARD_FILE_NAME = "shards.js";
const std::string SHARD_KEY_NAME = "shards";

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils::async;

using json = nlohmann::json;

class ShardEngine::Private : IEngine {
public:
  Private(const std::string &path) {

    _stoped = false;
    _settings = Settings::create(path);
    ThreadManager::Params tpm_params(_settings->thread_pools_params());
    ThreadManager::start(tpm_params);

    loadShardFile();
  }

  ~Private() { this->stop(); }

  void stop() {
    if (!_stoped) {
      logger_info("shards: stopping");
      _id2shard.clear();
      for (auto s : _sub_storages) {
        s->stop();
      }
      ThreadManager::stop();
    }
  }
  const std::string SHARD_KEY_PATH = "path";
  const std::string SHARD_KEY_NAME = "name";
  const std::string SHARD_KEY_IDS = "ids";

  std::string shardFileName() {
    return utils::fs::append_path(_settings->storage_path.value(),
                                  SHARD_FILE_NAME);
  }

  void loadShardFile() {
    auto fname = shardFileName();
    if (utils::fs::file_exists(fname)) {
      logger("shards: loading ", fname);
      std::string content = dariadb::utils::fs::read_file(fname);
      json js = json::parse(content);
      auto params_array = js[SHARD_KEY_NAME];

      for (auto kv : params_array) {
        auto param_path = kv[SHARD_KEY_PATH].get<std::string>();
        auto param_name = kv[SHARD_KEY_NAME].get<std::string>();
        auto param_ids = kv[SHARD_KEY_IDS].get<IdSet>();

        ShardEngine::Shard d{param_path, param_name, param_ids};

        shardAdd_inner(d);
      }
    }
  }

  void saveShardFile() {

    auto file = shardFileName();
    logger("shards: save to ", file);
    json js;

    for (auto &o : _shards) {
      json reccord = {{SHARD_KEY_PATH, o.path},
                      {SHARD_KEY_IDS, o.ids},
                      {SHARD_KEY_NAME, o.name}};
      js[SHARD_KEY_NAME].push_back(reccord);
    }

    std::fstream fs;
    fs.open(file, std::ios::out);
    if (!fs.is_open()) {
      throw MAKE_EXCEPTION("!fs.is_open()");
    }
    fs << js.dump(1);
    fs.flush();
    fs.close();
  }

  void shardAdd(const ShardEngine::Shard &d) {
    shardAdd_inner(d);
    saveShardFile();
  }

  void shardAdd_inner(const ShardEngine::Shard &d) {
    std::lock_guard<utils::async::Locker> lg(_locker);
    logger_info("shards: add new shard {", d.name, "} => ", d.path);
    _shards.push_back(d);
    auto shard_ptr = open_shard_path(d);
    _sub_storages.push_back(shard_ptr);

    if (d.ids.empty()) {
      ENSURE(_default_shard == nullptr);
      _default_shard = shard_ptr;
    } else {
      for (auto id : d.ids) {
        _id2shard[id] = shard_ptr;
      }
    }
  }

  std::list<Shard> shardList() {
    std::lock_guard<utils::async::Locker> lg(_locker);
    return std::list<Shard>(_shards.begin(), _shards.end());
  }

  IEngine_Ptr open_shard_path(const Shard &s) {
    ENSURE(!s.path.empty());
    auto settings = Settings::create(s.path);
    settings->alias = "(" + s.name + ")";
    IEngine_Ptr new_shard{new Engine(settings, false)};
    return new_shard;
  }

  IEngine_Ptr get_shard_for_id(Id id) {
    IEngine_Ptr target_shard = _default_shard;

    _locker.lock();
    auto fres = this->_id2shard.find(id);
    if (fres != this->_id2shard.end()) {
      target_shard = fres->second;
    }
    _locker.unlock();

    if (target_shard == nullptr) {
      logger_fatal("shard: shard for id:", id,
                   " not found. default shard is nullptr.");
      nullptr;
    }
    return target_shard;
  }

  Status append(const Meas &value) override {
    IEngine_Ptr target_shard = get_shard_for_id(value.id);

    if (target_shard == nullptr) {
      return Status(0, 1);
    } else {
      return target_shard->append(value);
    }
  }

  Time minTime() override {
    Time result = MAX_TIME;
    for (auto s : this->_sub_storages) {
      auto subres = s->minTime();
      result = std::min(subres, result);
    }
    return result;
  }

  Time maxTime() override {
    Time result = MIN_TIME;
    for (auto s : this->_sub_storages) {
      auto subres = s->maxTime();
      result = std::max(subres, result);
    }
    return result;
  }

  Id2MinMax loadMinMax() {
    Id2MinMax result;
    for (auto s : this->_sub_storages) {
      auto subres = s->loadMinMax();
      for (auto kv : subres) {
        result[kv.first] = kv.second;
      }
    }
    return result;
  }

  bool minMaxTime(Id id, Time *minResult, Time *maxResult) override {
    auto target_shard = get_shard_for_id(id);
    if (target_shard == nullptr) {
      return false;
    } else {
      return target_shard->minMaxTime(id, minResult, maxResult);
    }
  }

  void foreach (const QueryInterval &q, IReadCallback * clbk) override {
    auto cursors = intervalReader(q);
    for (auto id : q.ids) {
      auto iter = cursors.find(id);
      if (iter != cursors.end()) {
        iter->second->apply(clbk, q);
      }
    }
    clbk->is_end();
  }

  void foreach (const QueryTimePoint &q, IReadCallback * clbk) {
    auto values = this->readTimePoint(q);
    for (auto &kv : values) {
      if (clbk->is_canceled()) {
        break;
      }
      clbk->apply(kv.second);
    }
    clbk->is_end();
  }

  Id2Cursor intervalReader(const QueryInterval &q) override {
    Id2Cursor result;
    for (auto id : q.ids) {
      auto target_shard = get_shard_for_id(id);
      if (target_shard != nullptr) {
        QueryInterval local_q = q;
        local_q.ids.resize(1);
        local_q.ids[0] = id;
        auto subresult = target_shard->intervalReader(local_q);
        for (auto kv : subresult) {
          result[kv.first] = kv.second;
        }
      }
    }
    return result;
  }

  Id2Meas readTimePoint(const QueryTimePoint &q) override {
    Id2Meas result;
    for (auto id : q.ids) {
      auto target_shard = get_shard_for_id(id);
      if (target_shard != nullptr) {
        QueryTimePoint local_q = q;
        local_q.ids.resize(1);
        local_q.ids[0] = id;
        auto subresult = target_shard->readTimePoint(local_q);
        for (auto kv : subresult) {
          result[kv.first] = kv.second;
        }
      }
    }
    return result;
  }

  Id2Meas currentValue(const IdArray &ids, const Flag &flag) override {
    // TODO do more smarter: group id per id and do one query per shard.
    Id2Meas result;
    for (auto id : ids) {
      auto target_shard = get_shard_for_id(id);
      if (target_shard != nullptr) {
        auto subresult = target_shard->currentValue({id}, flag);
        for (auto kv : subresult) {
          result[kv.first] = kv.second;
        }
      }
    }
    return result;
  }

  Statistic stat(const Id id, Time from, Time to) override {
    auto target_shard = get_shard_for_id(id);
    if (target_shard == nullptr) {
      return Statistic();
    } else {
      return target_shard->stat(id, from, to);
    }
  }

  void fsck() override {}

  void eraseOld(const Time &t) override {}

  void repack() override {}

  bool _stoped;
  std::unordered_map<Id, IEngine_Ptr> _id2shard;
  std::list<ShardEngine::Shard> _shards;
  std::list<IEngine_Ptr> _sub_storages;
  IEngine_Ptr _default_shard;
  Settings_ptr _settings;
  utils::async::Locker _locker;
};

ShardEngine_Ptr ShardEngine::create(const std::string &path) {
  return ShardEngine_Ptr{new ShardEngine(path)};
}

ShardEngine::ShardEngine(const std::string &path)
    : _impl(new ShardEngine::Private(path)) {}

void ShardEngine::shardAdd(const Shard &d) { _impl->shardAdd(d); }

std::list<ShardEngine::Shard> ShardEngine::shardList() {
  return _impl->shardList();
}

Status ShardEngine::append(const Meas &value) { return _impl->append(value); }

Time ShardEngine::minTime() { return _impl->minTime(); }

Time ShardEngine::maxTime() { return _impl->maxTime(); }

Id2MinMax ShardEngine::loadMinMax() { return _impl->loadMinMax(); }

bool ShardEngine::minMaxTime(Id id, Time *minResult, Time *maxResult) {
  return _impl->minMaxTime(id, minResult, maxResult);
}

void ShardEngine::foreach (const QueryInterval &q, IReadCallback * clbk) {
  _impl->foreach (q, clbk);
}

void ShardEngine::foreach (const QueryTimePoint &q, IReadCallback * clbk) {
  _impl->foreach (q, clbk);
}

Id2Cursor ShardEngine::intervalReader(const QueryInterval &query) {
  return _impl->intervalReader(query);
}

Id2Meas ShardEngine::readTimePoint(const QueryTimePoint &q) {
  return _impl->readTimePoint(q);
}

Id2Meas ShardEngine::currentValue(const IdArray &ids, const Flag &flag) {
  return _impl->currentValue(ids, flag);
}

Statistic ShardEngine::stat(const Id id, Time from, Time to) {
  return _impl->stat(id, from, to);
}

void ShardEngine::fsck() { _impl->fsck(); }

void ShardEngine::eraseOld(const Time &t) { _impl->eraseOld(t); }

void ShardEngine::repack() { _impl->repack(); }

void ShardEngine::stop() { _impl->stop(); }
