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

class ShardEngine::Private : IMeasStorage, IEngine {
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
        auto param_path = kv["path"].get<std::string>();
        auto param_name = kv["name"].get<std::string>();
        auto param_ids = kv["ids"].get<IdSet>();

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
      json reccord = {{"path", o.path}, {"ids", o.ids}, {"name", o.name}};
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

  Time minTime() override { return Time(); }
  Time maxTime() override { return Time(); }

  bool minMaxTime(Id id, Time *minResult, Time *maxResult) override {
    return false;
  }

  void foreach (const QueryInterval &q, IReadCallback * clbk) override {}

  Id2Cursor intervalReader(const QueryInterval &query) override {
    return Id2Cursor();
  }

  Id2Meas readTimePoint(const QueryTimePoint &q) override { return Id2Meas(); }
  Id2Meas currentValue(const IdArray &ids, const Flag &flag) override {
    return Id2Meas();
  }
  Statistic stat(const Id id, Time from, Time to) override {
    return Statistic();
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

Time ShardEngine::minTime() { return _impl->minTime(); }

Time ShardEngine::maxTime() { return _impl->maxTime(); }

bool ShardEngine::minMaxTime(Id id, Time *minResult, Time *maxResult) {
  return _impl->minMaxTime(id, minResult, maxResult);
}

void ShardEngine::foreach (const QueryInterval &q, IReadCallback * clbk) {
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
