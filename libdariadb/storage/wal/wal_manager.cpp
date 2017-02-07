#include <libdariadb/flags.h>
#include <libdariadb/storage/callbacks.h>
#include <libdariadb/storage/cursors.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/storage/wal/wal_manager.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/utils.h>

#include <iterator>
#include <tuple>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils::async;

EXPORT WALManager *WALManager::_instance = nullptr;

WALManager::~WALManager() { this->flush(); }

WALManager_ptr WALManager::create(const EngineEnvironment_ptr env) {
  return WALManager_ptr{new WALManager(env)};
}

WALManager::WALManager(const EngineEnvironment_ptr env) {
  _env = env;
  _settings =
      _env->getResourceObject<Settings>(EngineEnvironment::Resource::SETTINGS);
  _down = nullptr;
  auto manifest =
      _env->getResourceObject<Manifest>(EngineEnvironment::Resource::MANIFEST);
  if (dariadb::utils::fs::path_exists(_settings->raw_path.value())) {
    auto wals = manifest->wal_list();
    for (auto f : wals) {
      auto full_filename =
          utils::fs::append_path(_settings->raw_path.value(), f);
      if (WALFile::writed(full_filename) != _settings->wal_file_size.value()) {
        logger_info("engine: WalManager open exist file ", f);
        WALFile_Ptr p = WALFile::open(_env, full_filename);
        _wal = p;
        break;
      }
    }
  }

  _buffer.resize(_settings->wal_cache_size.value());
  _buffer_pos = 0;
}

void WALManager::create_new() {
  _wal = nullptr;
  if (_settings->strategy.value() != STRATEGY::WAL) {
    drop_old_if_needed();
  }
  _wal = WALFile::create(_env);
}

void WALManager::dropAll() {
  if (_down != nullptr) {
    auto all_files = wal_files();
    this->_wal = nullptr;
    for (auto f : all_files) {
      auto without_path = utils::fs::extract_filename(f);
      if (_files_send_to_drop.find(without_path) == _files_send_to_drop.end()) {
        // logger_info("engine: drop ",without_path);
        this->dropWAL(f, _down);
      }
    }
  }
}

void WALManager::dropClosedFiles(size_t count) {
  if (_down != nullptr) {
    auto closed = this->closedWals();

    size_t to_drop = std::min(closed.size(), count);
    for (size_t i = 0; i < to_drop; ++i) {
      auto f = closed.front();
      closed.pop_front();
      auto without_path = utils::fs::extract_filename(f);
      if (_files_send_to_drop.find(without_path) == _files_send_to_drop.end()) {
        this->dropWAL(f, _down);
      }
    }
    // clean set of sended to drop files.
    auto manifest = _env->getResourceObject<Manifest>(
        EngineEnvironment::Resource::MANIFEST);
    auto wals_exists = manifest->wal_list();
    std::set<std::string> wal_exists_set{wals_exists.begin(),
                                         wals_exists.end()};
    std::set<std::string> new_sended_files;
    for (auto &v : _files_send_to_drop) {
      if (wal_exists_set.find(v) != wal_exists_set.end()) {
        new_sended_files.emplace(v);
      }
    }
    _files_send_to_drop = new_sended_files;
  }
}

void WALManager::drop_old_if_needed() {
  if (_settings->strategy.value() != STRATEGY::WAL) {
    auto closed = this->closedWals();
    dropClosedFiles(closed.size());
  }
}

std::list<std::string> WALManager::wal_files() const {
  std::list<std::string> res;
  auto files =
      _env->getResourceObject<Manifest>(EngineEnvironment::Resource::MANIFEST)
          ->wal_list();
  for (auto f : files) {
    auto full_path = utils::fs::append_path(_settings->raw_path.value(), f);
    res.push_back(full_path);
  }
  return res;
}

std::list<std::string> WALManager::closedWals() {
  auto all_files = wal_files();
  std::list<std::string> result;
  for (auto fn : all_files) {
    if (_wal == nullptr) {
      result.push_back(fn);
    } else {
      if (fn != this->_wal->filename()) {
        result.push_back(fn);
      }
    }
  }
  return result;
}

void WALManager::dropWAL(const std::string &fname, IWALDropper *storage) {
  WALFile_Ptr ptr = WALFile::open(_env, fname, false);
  auto without_path = utils::fs::extract_filename(fname);
  _files_send_to_drop.emplace(without_path);
  storage->dropWAL(without_path);
}

void WALManager::setDownlevel(IWALDropper *down) {
  _down = down;
  this->drop_old_if_needed();
}

dariadb::Time WALManager::minTime() {
  std::lock_guard<std::mutex> lg(_locker);
  auto files = wal_files();
  dariadb::Time result = dariadb::MAX_TIME;
  auto env = _env;
  AsyncTask at = [files, &result, env](const ThreadInfo &ti) {
    TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);
    for (auto filename : files) {
      auto wal = WALFile::open(env, filename, true);
      auto local = wal->minTime();
      result = std::min(local, result);
    }
    return false;
  };

  auto am_async =
      ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
  am_async->wait();

  size_t pos = 0;
  for (auto &v : _buffer) {
    result = std::min(v.time, result);
    ++pos;
    if (pos > _buffer_pos) {
      break;
    }
  }
  return result;
}

dariadb::Time WALManager::maxTime() {
  std::lock_guard<std::mutex> lg(_locker);
  auto files = wal_files();
  dariadb::Time result = dariadb::MIN_TIME;
  auto env = _env;
  AsyncTask at = [files, &result, env](const ThreadInfo &ti) {
    TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);
    for (auto filename : files) {
      auto wal = WALFile::open(env, filename, true);
      auto local = wal->maxTime();
      result = std::max(local, result);
    }
    return false;
  };

  auto am_async =
      ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
  am_async->wait();
  for (auto &v : _buffer) {
    result = std::max(v.time, result);
  }
  return result;
}

bool WALManager::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                            dariadb::Time *maxResult) {
  std::lock_guard<std::mutex> lg(_locker);
  auto files = wal_files();
  using MMRes = std::tuple<bool, dariadb::Time, dariadb::Time>;
  std::vector<MMRes> results{files.size()};
  auto env = _env;
  AsyncTask at = [files, &results, id, env](const ThreadInfo &ti) {
    TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);
    size_t num = 0;

    for (auto filename : files) {

      auto wal = WALFile::open(env, filename, true);
      dariadb::Time lmin = dariadb::MAX_TIME, lmax = dariadb::MIN_TIME;
      if (wal->minMaxTime(id, &lmin, &lmax)) {
        results[num] = MMRes(true, lmin, lmax);
      } else {
        results[num] = MMRes(false, lmin, lmax);
      }
      num++;
    }
    return false;
  };
  auto am_async =
      ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
  am_async->wait();

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

  size_t pos = 0;
  for (auto v : _buffer) {
    if (v.id == id) {
      res = true;
      *minResult = std::min(v.time, *minResult);
      *maxResult = std::max(v.time, *maxResult);
    }
    ++pos;
    if (pos > _buffer_pos) {
      break;
    }
  }
  return res;
}

Id2Cursor WALManager::intervalReader(const QueryInterval &q) {
  std::lock_guard<std::mutex> lg(_locker);
  Id2CursorsList readers_list;

  utils::async::Locker readers_locker;
  auto files = wal_files();

  if (!files.empty()) {
    auto env = _env;
    AsyncTask at = [files, &q, env, &readers_list,
                    &readers_locker](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);
      for (auto filename : files) {
        auto wal = WALFile::open(env, filename, true);

        auto rdr_map = wal->intervalReader(q);
        if (rdr_map.empty()) {
          continue;
        }
        for (auto kv : rdr_map) {
          std::lock_guard<utils::async::Locker> lg(readers_locker);
          readers_list[kv.first].push_back(kv.second);
        }
      }
      return false;
    };

    auto am_async =
        ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
    am_async->wait();
  }

  size_t pos = 0;
  Id2MSet i2ms;
  for (auto v : _buffer) {
    if (pos >= _buffer_pos) {
      break;
    }
    if (v.inQuery(q.ids, q.flag, q.from, q.to)) {
      i2ms[v.id].insert(v);
    }
    ++pos;
  }

  if (!i2ms.empty()) {
    for (auto kv : i2ms) {
      MeasArray ma(kv.second.begin(), kv.second.end());
      std::sort(ma.begin(), ma.end(), meas_time_compare_less());
      ENSURE(ma.front().time <= ma.back().time);
      FullCursor *fr = new FullCursor(ma);
      Cursor_Ptr r{fr};
      readers_list[kv.first].push_back(r);
    }
  }
  return CursorWrapperFactory::colapseCursors(readers_list);
}

Statistic WALManager::stat(const Id id, Time from, Time to) {
  std::lock_guard<std::mutex> lg(_locker);
  Statistic result;

  auto files = wal_files();

  if (!files.empty()) {
    auto env = _env;
    AsyncTask at = [files, &result, id, from, to, env](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);
      for (auto filename : files) {
        auto wal = WALFile::open(env, filename, true);

        auto st = wal->stat(id, from, to);
        result.update(st);
      }
      return false;
    };

    auto am_async =
        ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
    am_async->wait();
  }

  size_t pos = 0;
  IdArray ids{ id };
  ENSURE(ids[0] == id);
  for (auto v : _buffer) {
	  if (pos >= _buffer_pos) {
		  break;
	  }
	  if (v.inQuery(ids, Flag(0), from, to)) {
		  result.update(v);
	  }
	  ++pos;
  }
  return result;
}

void WALManager::foreach (const QueryInterval &q, IReadCallback * clbk) {
  auto reader = intervalReader(q);
  for (auto kv : reader) {
    kv.second->apply(clbk);
  }
}

Id2Meas WALManager::readTimePoint(const QueryTimePoint &query) {
  std::lock_guard<std::mutex> lg(_locker);
  auto files = wal_files();
  dariadb::Id2Meas sub_result;

  std::vector<Id2Meas> results{files.size()};
  auto env = _env;
  AsyncTask at = [files, &query, &results, env](const ThreadInfo &ti) {
    TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);
    size_t num = 0;

    for (auto filename : files) {
      auto wal = WALFile::open(env, filename, true);
      results[num] = wal->readTimePoint(query);
      num++;
    }
    return false;
  };

  auto am_async =
      ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
  am_async->wait();

  for (auto &out : results) {
    for (auto &kv : out) {
      auto it = sub_result.find(kv.first);
      if (it == sub_result.end()) {
        sub_result.emplace(std::make_pair(kv.first, kv.second));
      } else {
        if (it->second.flag == FLAGS::_NO_DATA) {
          sub_result[kv.first] = kv.second;
        }
      }
    }
  }
  size_t pos = 0;
  for (auto v : _buffer) {
    if (v.inQuery(query.ids, query.flag)) {
      auto it = sub_result.find(v.id);
      if (it == sub_result.end()) {
        sub_result.emplace(std::make_pair(v.id, v));
      } else {
        if ((v.flag == FLAGS::_NO_DATA) ||
            ((v.time > it->second.time) && (v.time <= query.time_point))) {
          sub_result[v.id] = v;
        }
      }
    }
    ++pos;
    if (pos > _buffer_pos) {
      break;
    }
  }

  for (auto id : query.ids) {
    if (sub_result.find(id) == sub_result.end()) {
      sub_result[id].flag = FLAGS::_NO_DATA;
      sub_result[id].time = query.time_point;
    }
  }
  return sub_result;
}

Id2Meas WALManager::currentValue(const IdArray &ids, const Flag &flag) {
  dariadb::Id2Meas meases;
  AsyncTask at = [&ids, flag, &meases, this](const ThreadInfo &ti) {
    TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);

    auto files = wal_files();

    for (const auto &f : files) {
      auto c = WALFile::open(_env, f, true);
      auto sub_rdr = c->currentValue(ids, flag);

      for (auto &kv : sub_rdr) {
        auto it = meases.find(kv.first);
        if (it == meases.end()) {
          meases.emplace(std::make_pair(kv.first, kv.second));
        } else {
          if ((it->second.flag == FLAGS::_NO_DATA) ||
              (it->second.time < kv.second.time)) {
            meases[kv.first] = kv.second;
          }
        }
      }
    }
    return false;
  };
  auto am_async =
      ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
  am_async->wait();
  return meases;
}

dariadb::Status WALManager::append(const Meas &value) {
  std::lock_guard<std::mutex> lg(_locker);
  _buffer[_buffer_pos] = value;
  _buffer_pos++;

  if (_buffer_pos >= _settings->wal_cache_size.value()) {
    flush_buffer();
  }
  return dariadb::Status(1, 0);
}

void WALManager::flush_buffer() {
  if (_buffer_pos == size_t(0)) {
    return;
  }
  AsyncTask at = [this](const ThreadInfo &ti) {
    TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);
    if (_wal == nullptr) {
      create_new();
    }
    size_t pos = 0;
    size_t total_writed = 0;
    while (1) {
      auto res =
          _wal->append(_buffer.begin() + pos, _buffer.begin() + _buffer_pos);
      total_writed += res.writed;
      if (total_writed != _buffer_pos) {
        create_new();
        pos += res.writed;
      } else {
        break;
      }
    }
    _buffer_pos = 0;
    return false;
  };
  auto async_r = ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
  async_r->wait();
}

void WALManager::flush() { flush_buffer(); }

size_t WALManager::filesCount() const { return wal_files().size(); }

void WALManager::erase(const std::string &fname) {
  _env->getResourceObject<Manifest>(EngineEnvironment::Resource::MANIFEST)
      ->wal_rm(fname);
  utils::fs::rm(utils::fs::append_path(_settings->raw_path.value(), fname));
}

Id2MinMax WALManager::loadMinMax() {
  auto files = wal_files();

  dariadb::Id2MinMax result;
  for (const auto &f : files) {
    auto c = WALFile::open(_env, f, true);
    auto sub_res = c->loadMinMax();

    minmax_append(result, sub_res);
  }
  return result;
}
