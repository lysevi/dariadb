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

WALManager::~WALManager() {
  this->flush();
}

WALManager_ptr WALManager::create(const EngineEnvironment_ptr env) {
  return WALManager_ptr{new WALManager(env)};
}

WALManager::WALManager(const EngineEnvironment_ptr env) {
  _env = env;
  _settings = _env->getResourceObject<Settings>(EngineEnvironment::Resource::SETTINGS);
  _down = nullptr;
  auto manifest =
      _env->getResourceObject<Manifest>(EngineEnvironment::Resource::MANIFEST);
  if (dariadb::utils::fs::path_exists(_settings->raw_path.value())) {
    auto wals = manifest->wal_list();
    for (auto f : wals) {
      auto full_filename = utils::fs::append_path(_settings->raw_path.value(), f.fname);

      WALFile_Ptr p = WALFile::open(_env, full_filename);
      auto writed = p->writed();
      if (writed == 0) {
        p = nullptr;
        this->erase(f.fname);
      }
      _file2minmax[full_filename].minTime = p->minTime();
      _file2minmax[full_filename].maxTime = p->maxTime();
      _file2minmax[full_filename].bloom_id = p->id_bloom();

      if (WALFile::writed(full_filename) != _settings->wal_file_size.value()) {
        logger_info("engine", _settings->alias, ": WalManager open exist file ", f.fname);

        _wal[p->id_from_first()] = p;
      }
    }
  }

  //_buffer.resize(_settings->wal_cache_size.value());
  //_buffer_pos = 0;
}

WALFile_Ptr WALManager::create_new(dariadb::Id id) {
  auto fres = _wal.find(id);
  if (fres != _wal.end()) {
    std::lock_guard<std::mutex> lg(_file2mm_locker);
    auto walfile_ptr = fres->second;
    auto f = walfile_ptr->filename();
    _file2minmax[f].minTime = walfile_ptr->minTime();
    _file2minmax[f].maxTime = walfile_ptr->maxTime();
    _file2minmax[f].bloom_id = walfile_ptr->id_bloom();
    if (_settings->strategy.value() != STRATEGY::WAL) {
      dropFile(f);
    }
    _wal.erase(fres);
  }

  auto result = WALFile::create(_env, id);
  _wal[id] = result;
  logger_info("create #", id, " => ", result->filename());
  return result;
}

void WALManager::dropAll() {
  if (_down != nullptr) {
    _global_lock.lock();
    this->flush();
    for (auto kv : _wal) {
      if (kv.second != nullptr) {
        auto fname = kv.second->filename();
        dropFile(fname);
      }
    }
    _wal.clear();
    auto all_files = wal_files_all();

    for (auto f : all_files) {
      dropFile(f);
    }
    _global_lock.unlock();
  }
}

void WALManager::dropFile(const std::string &f) {
  if (_down != nullptr) {
    auto without_path = utils::fs::extract_filename(f);
    if (_files_send_to_drop.find(without_path) == _files_send_to_drop.end()) {
      this->dropWAL(f, _down);
    }
    auto manifest =
        _env->getResourceObject<Manifest>(EngineEnvironment::Resource::MANIFEST);
    auto wals_exists = manifest->wal_list();
    std::set<std::string> wal_exists_set;
    for (auto we : wals_exists) {
      wal_exists_set.insert(we.fname);
    }
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
    auto all_files = wal_files_all();
    for (auto f : all_files) {
      bool exists = false;
      for (auto kv : _wal) {
        if (kv.second != nullptr && kv.second->filename() == f) {
          exists = true;
          break;
        }
      }
      if (!exists) {
        dropFile(f);
      }
    }
  }
}

std::list<std::string> WALManager::wal_files(dariadb::Id id) const {
  std::list<std::string> res;
  auto files = _env->getResourceObject<Manifest>(EngineEnvironment::Resource::MANIFEST)
                   ->wal_list(id);
  for (auto f : files) {
    auto full_path = utils::fs::append_path(_settings->raw_path.value(), f.fname);
    res.push_back(full_path);
  }
  return res;
}

std::list<std::string> WALManager::wal_files_all() const {
  std::list<std::string> res;
  auto files = _env->getResourceObject<Manifest>(EngineEnvironment::Resource::MANIFEST)
                   ->wal_list();
  for (auto f : files) {
    auto full_path = utils::fs::append_path(_settings->raw_path.value(), f.fname);
    res.push_back(full_path);
  }
  return res;
}

std::list<std::string> WALManager::closedWals() {
  auto all_files = wal_files_all();
  std::list<std::string> result;
  for (auto fn : all_files) {
    if (_wal.empty()) {
      result.push_back(fn);
    } else {
      for (auto kv : _wal) {
        if (kv.second->filename() == fn) {
          continue;
        }
      }
      result.push_back(fn);
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
  std::shared_lock<std::shared_mutex> lg(_global_lock);
  auto files = wal_files_all();
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

  auto am_async = ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
  am_async->wait();

  for (auto pos_kv : _buffer_pos) {
    auto target_buffer = &_buffer[pos_kv.first];
    for (size_t pos = 0; pos < pos_kv.second; ++pos) {
      result = std::min(target_buffer->at(pos).time, result);
    }
  }
  return result;
}

dariadb::Time WALManager::maxTime() {
  std::shared_lock<std::shared_mutex> lg(_global_lock);
  auto files = wal_files_all();
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

  auto am_async = ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
  am_async->wait();
  for (auto pos_kv : _buffer_pos) {
    auto target_buffer = &_buffer[pos_kv.first];
    for (size_t pos = 0; pos < pos_kv.second; ++pos) {
      result = std::max(target_buffer->at(pos).time, result);
    }
  }
  return result;
}

bool WALManager::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                            dariadb::Time *maxResult) {
  std::shared_lock<std::shared_mutex> lg(_global_lock);
  auto files = wal_files(id);
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
  auto am_async = ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
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

  auto pos_fres = _buffer_pos.find(id);
  if (pos_fres != _buffer_pos.end()) {
    auto target_buffer = &_buffer[pos_fres->first];
    for (size_t pos = 0; pos < pos_fres->second; ++pos) {
      Meas v = target_buffer->at(pos);
      res = true;
      *minResult = std::min(v.time, *minResult);
      *maxResult = std::max(v.time, *maxResult);
    }
  }
  return res;
}

void WALManager::intervalReader_async_logic(const std::list<std::string> &files,
                                            const QueryInterval &q,
                                            Id2CursorsList &readers_list,
                                            utils::async::Locker &readers_locker) {
  for (auto filename : files) {
    if (!file_in_query(filename, q)) {
      continue;
    }
    auto wal = WALFile::open(_env, filename, true);

    auto rdr_map = wal->intervalReader(q);
    if (rdr_map.empty()) {
      continue;
    }
    for (auto kv : rdr_map) {
      std::lock_guard<utils::async::Locker> lg(readers_locker);
      readers_list[kv.first].push_back(kv.second);
    }
  }
}

Id2Cursor WALManager::intervalReader(const QueryInterval &q) {
  std::shared_lock<std::shared_mutex> lg(_global_lock);
  Id2CursorsList readers_list;

  QueryInterval local_q = q;
  local_q.ids.resize(1);

  for (auto id : q.ids) {

    local_q.ids[0] = id;
    auto files = wal_files(id);
    if (!files.empty()) {
      utils::async::Locker readers_locker;

      AsyncTask at = [files, &local_q, &readers_list, &readers_locker,
                      this](const ThreadInfo &ti) {
        TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);
        this->intervalReader_async_logic(files, local_q, readers_list, readers_locker);
        return false;
      };

      auto am_async = ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
      am_async->wait();
    }
  }

  size_t pos = 0;
  Id2MSet i2ms;
  for (auto id : q.ids) {
    auto pos_it = _buffer_pos.find(id);
    if (pos_it == _buffer_pos.end()) {
      continue;
    }
    auto target_buffer = &_buffer[pos_it->first];
    for (size_t i = 0; i < pos_it->second; ++i) {
      Meas v = target_buffer->at(i);
      if (v.inQuery(q.ids, q.flag, q.from, q.to)) {
        i2ms[v.id].insert(v);
      }
      ++pos;
    }
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
  std::shared_lock<std::shared_mutex> lg(_global_lock);
  Statistic result;

  auto files = wal_files(id);

  if (!files.empty()) {
    AsyncTask at = [files, &result, id, from, to, this](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);
      QueryInterval qi({id}, dariadb::Flag(), from, to);
      for (auto filename : files) {
        if (!this->file_in_query(filename, qi)) {
          continue;
        }
        auto wal = WALFile::open(this->_env, filename, true);

        auto st = wal->stat(id, from, to);
        result.update(st);
      }
      /*auto wal_iter = this->_wal.find(id);
      if (wal_iter != this->_wal.end()) {
        auto st = wal_iter->second->stat(id, from, to);
        result.update(st);
      }*/
      return false;
    };

    auto am_async = ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
    am_async->wait();
  }

  IdArray ids{id};
  ENSURE(ids[0] == id);
  auto pos_it = _buffer_pos.find(id);
  if (pos_it != _buffer_pos.end()) {
    auto pos = pos_it->second;
    auto target_buffer = &_buffer[pos_it->first];
    for (size_t i = 0; i < pos; ++i) {
      Meas v = target_buffer->at(i);
      if (v.inInterval(from, to)) {
        result.update(v);
      }
    }
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
  std::shared_lock<std::shared_mutex> lg(_global_lock);

  QueryTimePoint local_q = query;
  local_q.ids.resize(1);
  std::list<Id2Meas> results;
  dariadb::Id2Meas sub_result;

  for (auto id : query.ids) {
    auto files = wal_files(id);

    if (files.empty()) {
      continue;
    }

    local_q.ids[0] = id;

    auto env = _env;
    AsyncTask at = [files, &local_q, &results, env, this](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);

      for (auto filename : files) {
        if (!this->file_in_query(filename, local_q)) {
          continue;
        }
        auto wal = WALFile::open(env, filename, true);
        results.push_back(wal->readTimePoint(local_q));
      }
      return false;
    };

    auto am_async = ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
    am_async->wait();
  }
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
  for (auto id : query.ids) {
    auto pos_it = _buffer_pos.find(id);
    if (pos_it == _buffer_pos.end()) {
      continue;
    }
    auto target_buffer = &_buffer[pos_it->first];
    for (size_t i = 0; i < pos_it->second; ++i) {
      Meas v = target_buffer->at(i);
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

    for (auto id : ids) {
      auto files = wal_files(id);

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
    }
    return false;
  };
  auto am_async = ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
  am_async->wait();
  return meases;
}

dariadb::Status WALManager::append(const Meas &value) {
  _global_lock.lock_shared();
  bool creation_lock = false;
  auto pos_it = _buffer_pos.find(value.id);
  if (pos_it == _buffer_pos.end()) {
    _global_lock.unlock_shared();
    _global_lock.lock();
    pos_it = _buffer_pos.find(value.id);
    if (pos_it == _buffer_pos.end()) {
      _buffer_pos[value.id] = size_t(1);
      _buffer[value.id].resize(_settings->wal_cache_size.value());
      _buffer[value.id][0] = value;

      _lockers[value.id].lock();
      _lockers[value.id].unlock();

      _global_lock.unlock();
      return dariadb::Status(1);
    } else {
      creation_lock = true;
    }
  } else {
    _lockers[value.id].lock();

    _buffer[pos_it->first][pos_it->second] = value;
    pos_it->second++;
    if (pos_it->second >= _settings->wal_cache_size.value()) {
      flush_buffer(pos_it->first);
    } else {
      _lockers[value.id].unlock();
    }
  }
  if (creation_lock) {
    _global_lock.unlock();
  } else {
    _global_lock.unlock_shared();
  }
  return dariadb::Status(1);
}

void WALManager::flush_buffer(dariadb::Id id, bool sync) {
  /*if (_buffer_pos == size_t(0)) {
    return;
  }*/
  AsyncTask at = [this, id](const ThreadInfo &ti) {
    TKIND_CHECK(THREAD_KINDS::DISK_IO, ti.kind);
    if (_buffer_pos[id] == 0) {
      _lockers[id].unlock();
      return false;
    }
    size_t pos = 0;
    size_t total_writed = 0;
    if (_wal.find(id) == _wal.end()) {
      create_new(id);
    }
    auto target_buffer = &_buffer[id];
    while (1) {
      auto it = target_buffer->begin();
      auto begin = it + pos;
      auto end = it + _buffer_pos[id];
      Status res;
      res = _wal[id]->append(begin, end);
      total_writed += res.writed;
      if (res.error == APPEND_ERROR::wal_file_limit) {
        create_new(id);
      } else {
        if (res.error != APPEND_ERROR::OK) {
          logger_fatal("engine", this->_settings->alias, ": append to wal error - ",
                       res.error);
          return false;
        }
      }
      if (total_writed != _buffer_pos[id]) {
        pos += res.writed;
      } else {
        break;
      }
    }
    _buffer_pos[id] = size_t(0);
    std::fill_n(_buffer[id].begin(), _buffer[id].size(), Meas());
    _lockers[id].unlock();
    return false;
  };
  auto handle = ThreadManager::instance()->post(THREAD_KINDS::DISK_IO, AT(at));
  if (sync) {
    handle->wait();
  }
}

void WALManager::flush() {
  for (auto kv : _buffer_pos) {
    if (kv.second != size_t(0)) {
      flush_buffer(kv.first, true);
    }
  }
}

size_t WALManager::filesCount() const {
  return wal_files_all().size();
}

void WALManager::erase(const std::string &fname) {
  std::lock_guard<std::mutex> lg(_file2mm_locker);
  auto full_path = utils::fs::append_path(_settings->raw_path.value(), fname);
  _env->getResourceObject<Manifest>(EngineEnvironment::Resource::MANIFEST)->wal_rm(fname);
  _file2minmax.erase(full_path);
  utils::fs::rm(full_path);
}

Id2MinMax_Ptr WALManager::loadMinMax() {
  auto files = wal_files_all();

  auto result = std::make_shared<dariadb::Id2MinMax>();
  for (const auto &f : files) {
    auto c = WALFile::open(_env, f, true);
    auto sub_res = c->loadMinMax();

    minmax_append(result, sub_res);
  }

  for (auto pos_it : _buffer_pos) {
    auto target_buffer = &_buffer[pos_it.first];
    for (size_t i = 0; i < pos_it.second; ++i) {
      Meas val = target_buffer->at(i);
      auto fres = result->find_bucket(val.id);

      fres.v->second.updateMax(val);
      fres.v->second.updateMin(val);
    }
  }

  return result;
}

bool WALManager::file_in_query(const std::string &filename, const QueryInterval &q) {
  std::lock_guard<std::mutex> lg(_file2mm_locker);
  auto min_max_iter = this->_file2minmax.find(filename);
  if (min_max_iter == this->_file2minmax.end()) {
    return true;
  }
  bool intevalCheck = utils::inInterval(min_max_iter->second.minTime,
                                        min_max_iter->second.maxTime, q.from) ||
                      utils::inInterval(min_max_iter->second.minTime,
                                        min_max_iter->second.maxTime, q.to) ||
                      utils::inInterval(q.from, q.to, min_max_iter->second.minTime) ||
                      utils::inInterval(q.from, q.to, min_max_iter->second.maxTime);
  if (!intevalCheck) {
    return false;
  }
  for (auto id : q.ids) {
    bool bloom_result = bloom_check<Id>(min_max_iter->second.bloom_id, id);
    if (bloom_result) {
      return true;
    }
  }
  return false;
}

bool WALManager::file_in_query(const std::string &filename, const QueryTimePoint &q) {
  std::lock_guard<std::mutex> lg(_file2mm_locker);
  auto min_max_iter = this->_file2minmax.find(filename);
  if (min_max_iter == this->_file2minmax.end()) {
    return true;
  }
  if (min_max_iter != this->_file2minmax.end() &&
      min_max_iter->second.minTime > q.time_point) {
    return false;
  }
  if (min_max_iter != this->_file2minmax.end()) {
    bool bloom_result = false;
    for (auto id : q.ids) {
      bloom_result = bloom_check<Id>(min_max_iter->second.bloom_id, id);
      if (bloom_result) {
        return true;
      }
    }
  }
  return false;
}
