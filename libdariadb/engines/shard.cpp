#include <extern/json/src/json.hpp>

#include <libdariadb/engines/engine.h>
#include <libdariadb/engines/shard.h>
#include <libdariadb/storage/subscribe.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <libdariadb/utils/fs.h>
#include <shared_mutex>

#include <fstream>

const std::string SHARD_KEY_NAME = "shards";
const std::string SHARD_KEY_PATH = "path";
const std::string SHARD_KEY_ALIAS = "alias";
const std::string SHARD_KEY_IDS = "ids";

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils::async;

using json = nlohmann::json;

class ShardEngine::Private : IEngine {
  struct ShardRef {
    std::string path;
    IEngine_Ptr storage;
  };

public:
  Private(const std::string &path) {

    _stoped = false;
    _settings = Settings::create(path);
    ThreadManager::Params tpm_params(_settings->thread_pools_params());
    ThreadManager::start(tpm_params);
    _subscribe_notify.start();

    loadShardFile();
  }

  ~Private() { this->stop(); }

  void stop() {
    if (!_stoped) {
      logger_info("shards: stopping");
      _subscribe_notify.stop();
      _id2shard.clear();
      for (auto s : _sub_storages) {
        s.storage->stop();
      }
      ThreadManager::stop();
    }
  }

  std::string shardFileName() {
    return utils::fs::append_path(_settings->storage_path.value(), SHARD_FILE_NAME);
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
        auto param_name = kv[SHARD_KEY_ALIAS].get<std::string>();
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

    for (auto &kv : _shards) {
      json reccord = {{SHARD_KEY_PATH, kv.second.path},
                      {SHARD_KEY_IDS, kv.second.ids},
                      {SHARD_KEY_ALIAS, kv.second.alias}};
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

  void shardRm(const std::string &alias, bool rm_shard_folder) {
    std::lock_guard<std::shared_mutex> lg(_locker);
    logger_info("shards: rm shard {", alias, "}");

    auto f_iter = _shards.find(alias);
    if (f_iter != _shards.end()) {
      for (auto iter = _sub_storages.begin(); iter != _sub_storages.end(); ++iter) {
        auto ss = iter->storage;
        if (iter->path == f_iter->second.path) {
          ss->stop();
          _sub_storages.erase(iter);
          if (_default_shard.get() == ss.get()) {
            _default_shard = nullptr;
          }
          break;
        }
      }
      if (rm_shard_folder) {
        logger_info("shards: rm path - ", f_iter->second.path);
        utils::fs::rm(f_iter->second.path);
      }
      for (auto id : f_iter->second.ids) {
        _id2shard.erase(id);
      }
      _shards.erase(f_iter);
      saveShardFile();
    } else {
      logger_info("shards: rm - shard with alias={", alias, "} not found.");
    }
  }

  void shardAdd_inner(const ShardEngine::Shard &d) {
    std::lock_guard<std::shared_mutex> lg(_locker);
    logger_info("shards: add new shard {", d.alias, "} => ", d.path);
    auto f_iter = _shards.find(d.alias);

    IEngine_Ptr shard_ptr = nullptr;
    if (f_iter != _shards.end()) {
      Shard new_shard_rec(d);

      for (auto &sr : _sub_storages) {
        if (sr.path == d.path) {
          shard_ptr = sr.storage;
          break;
        }
      }
      ENSURE(shard_ptr != nullptr);
      for (auto id : f_iter->second.ids) {
        new_shard_rec.ids.insert(id);
      }
      _shards.erase(f_iter);
      _shards.insert(std::make_pair(d.alias, new_shard_rec));
    } else {
      _shards.insert(std::make_pair(d.alias, d));
      shard_ptr = open_shard_path(d);
      _sub_storages.push_back({d.path, shard_ptr});
    }

    if (d.ids.empty()) {
      ENSURE(_default_shard == nullptr);
      _default_shard = shard_ptr;
    } else {
      for (auto id : d.ids) {
        if (_id2shard.count(id) == 0) {
          _id2shard[id] = shard_ptr;
        }
      }
    }
  }

  std::list<Shard> shardList() {
    std::shared_lock<std::shared_mutex> lg(_locker);
    std::list<Shard> result;
    for (auto kv : _shards) {
      result.push_back(kv.second);
    }
    return result;
  }

  IEngine_Ptr open_shard_path(const Shard &s) {
    ENSURE(!s.path.empty());
    auto settings = Settings::create(s.path);
    settings->alias = "(" + s.alias + ")";
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
      logger_fatal("shard: shard for id:", id, " not found. default shard is nullptr.");
      return nullptr;
    }
    return target_shard;
  }

  Status append(const Meas &value) override {
    IEngine_Ptr target_shard = get_shard_for_id(value.id);

    if (target_shard == nullptr) {
      return Status(1, APPEND_ERROR::bad_shard);
    } else {
      auto result = target_shard->append(value);
      if (result.writed == 1) {
        _subscribe_notify.on_append(value);
      }
      return result;
    }
  }

  Time minTime() override {
    std::shared_lock<std::shared_mutex> lg(_locker);
    Time result = MAX_TIME;
    for (auto &s : this->_sub_storages) {
      auto subres = s.storage->minTime();
      result = std::min(subres, result);
    }
    return result;
  }

  Time maxTime() override {
    std::shared_lock<std::shared_mutex> lg(_locker);
    Time result = MIN_TIME;
    for (auto &s : this->_sub_storages) {
      auto subres = s.storage->maxTime();
      result = std::max(subres, result);
    }
    return result;
  }

  Id2MinMax_Ptr loadMinMax() {
    std::shared_lock<std::shared_mutex> lg(_locker);
    Id2MinMax_Ptr result = std::make_shared<Id2MinMax>();
    for (auto &s : this->_sub_storages) {
      auto subres = s.storage->loadMinMax();
      auto f = [&result](const Id2MinMax::value_type &v) {
        result->insert(v.first, v.second);
      };
      subres->apply(f);
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

  std::unordered_map<IEngine_Ptr, IdSet> makeStorage2iset(const IdArray &ids) {
    std::unordered_map<IEngine_Ptr, IdSet> result;
    for (auto id : ids) {
      auto target_shard = get_shard_for_id(id);
      if (target_shard != nullptr) {
        result[target_shard].insert(id);
      }
    }

    ENSURE(result.size() <= this->_sub_storages.size());
    return result;
  }

  Id2Cursor intervalReader(const QueryInterval &q) override {
    Id2Cursor result;
    std::unordered_map<IEngine_Ptr, IdSet> storage2iset = makeStorage2iset(q.ids);

    for (auto kv : storage2iset) {
      auto target_shard = kv.first;
      for (auto id : kv.second) {
        QueryInterval local_q = q;
        local_q.ids.resize(1);
        local_q.ids[0] = id;
        auto subresult = target_shard->intervalReader(local_q);
        for (auto id2cursor : subresult) {
          result[id2cursor.first] = id2cursor.second;
        }
      }
    }
    return result;
  }

  Id2Meas readTimePoint(const QueryTimePoint &q) override {
    Id2Meas result;
    std::unordered_map<IEngine_Ptr, IdSet> storage2iset = makeStorage2iset(q.ids);
    for (auto kv : storage2iset) {
      auto target_shard = kv.first;
      for (auto id : kv.second) {
        QueryTimePoint local_q = q;
        local_q.ids.resize(1);
        local_q.ids[0] = id;
        auto subresult = target_shard->readTimePoint(local_q);
        for (auto id2time : subresult) {
          result[id2time.first] = id2time.second;
        }
      }
    }
    return result;
  }

  Id2Meas currentValue(const IdArray &ids, const Flag &flag) override {
    Id2Meas result;
    std::unordered_map<IEngine_Ptr, IdSet> storage2iset = makeStorage2iset(ids);
    for (auto kv : storage2iset) {
      auto target_shard = kv.first;
      for (auto id : kv.second) {
        auto subresult = target_shard->currentValue({id}, flag);
        for (auto id2time : subresult) {
          result[id2time.first] = id2time.second;
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

  void fsck() override {
    std::shared_lock<std::shared_mutex> lg(_locker);
    for (auto &s : _sub_storages) {
      s.storage->fsck();
    }
  }

  void eraseOld(const Id id, const Time t) override {
    auto target = this->get_shard_for_id(id);
    if (target != nullptr) {
      target->eraseOld(id, t);
    }
  }

  void repack(dariadb::Id id) override {
    auto target = this->get_shard_for_id(id);
    if (target != nullptr) {
      target->repack(id);
    }
  }

  void compact(ICompactionController *logic) override {
    auto target = this->get_shard_for_id(logic->targetId);
    if (target != nullptr) {
      target->compact(logic);
    }
  }

  Description description() const override {
    std::shared_lock<std::shared_mutex> lg(_locker);
    Description result;
    for (auto &s : _sub_storages) {
      auto d = s.storage->description();
      result.update(d);
    }
    result.active_works = ThreadManager::instance()->active_works();
    return result;
  }

  void wait_all_asyncs() override { ThreadManager::instance()->flush(); }

  void compress_all() override {
    std::shared_lock<std::shared_mutex> lg(_locker);
    for (auto &s : _sub_storages) {
      s.storage->compress_all();
    }
  }

  storage::Settings_ptr settings() override { return _settings; }

  STRATEGY strategy() const override { return STRATEGY::SHARD; }

  void subscribe(const IdArray &ids, const Flag &flag,
                 const ReaderCallback_ptr &clbk) override {
    auto new_s = std::make_shared<SubscribeInfo>(ids, flag, clbk);
    _subscribe_notify.add(new_s);
  }

  bool _stoped;
  std::unordered_map<Id, IEngine_Ptr> _id2shard;
  std::unordered_map<std::string, ShardEngine::Shard> _shards; // alias => shard

  std::list<ShardRef> _sub_storages;
  IEngine_Ptr _default_shard;
  Settings_ptr _settings;
  mutable std::shared_mutex _locker;

  SubscribeNotificator _subscribe_notify;
};

ShardEngine_Ptr ShardEngine::create(const std::string &path) {
  return ShardEngine_Ptr{new ShardEngine(path)};
}

ShardEngine::ShardEngine(const std::string &path)
    : _impl(new ShardEngine::Private(path)) {}

void dariadb::ShardEngine::compress_all() {
  _impl->compress_all();
}

void dariadb::ShardEngine::wait_all_asyncs() {
  _impl->wait_all_asyncs();
}

IEngine::Description dariadb::ShardEngine::description() const {
  return _impl->description();
}

void ShardEngine::shardAdd(const Shard &d) {
  _impl->shardAdd(d);
}

void ShardEngine::shardRm(const std::string &alias, bool rm_shard_folder) {
  _impl->shardRm(alias, rm_shard_folder);
}

std::list<ShardEngine::Shard> ShardEngine::shardList() {
  return _impl->shardList();
}

Status ShardEngine::append(const Meas &value) {
  return _impl->append(value);
}

Time ShardEngine::minTime() {
  return _impl->minTime();
}

Time ShardEngine::maxTime() {
  return _impl->maxTime();
}

Id2MinMax_Ptr ShardEngine::loadMinMax() {
  return _impl->loadMinMax();
}

bool ShardEngine::minMaxTime(Id id, Time *minResult, Time *maxResult) {
  return _impl->minMaxTime(id, minResult, maxResult);
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

void ShardEngine::fsck() {
  _impl->fsck();
}

void ShardEngine::eraseOld(const Id id, const Time t) {
  _impl->eraseOld(id, t);
}

void ShardEngine::repack(dariadb::Id id) {
  _impl->repack(id);
}
void ShardEngine::compact(ICompactionController *logic) {
  _impl->compact(logic);
}

void ShardEngine::stop() {
  _impl->stop();
}

storage::Settings_ptr ShardEngine::settings() {
  return _impl->settings();
}

STRATEGY ShardEngine::strategy() const {
  return _impl->strategy();
}

void ShardEngine::subscribe(const IdArray &ids, const Flag &flag,
                            const ReaderCallback_ptr &clbk) {
  return _impl->subscribe(ids, flag, clbk);
}