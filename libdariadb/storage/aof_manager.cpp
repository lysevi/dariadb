#include "aof_manager.h"
#include "../flags.h"
#include "../utils/exception.h"
#include "../utils/fs.h"
#include "../utils/logger.h"
#include "../utils/metrics.h"
#include "../utils/thread_manager.h"
#include "callbacks.h"
#include "manifest.h"
#include "options.h"

#include <cassert>
#include <iterator>
#include <tuple>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils::async;

AOFManager *AOFManager::_instance = nullptr;

AOFManager::~AOFManager() {
  this->flush();
}

AOFManager::AOFManager() {
  _down = nullptr;

  if (dariadb::utils::fs::path_exists(Options::instance()->path)) {
    auto aofs = Manifest::instance()->aof_list();
    for (auto f : aofs) {
      auto full_filename = utils::fs::append_path(Options::instance()->path, f);
      if (AOFile::writed(full_filename) != Options::instance()->aof_max_size) {
        logger_info("engine: AofManager open exist file ", f);
        AOFile_Ptr p{new AOFile(full_filename)};
        _aof = p;
        break;
      }
    }
  }

  _buffer.resize(Options::instance()->aof_buffer_size);
  _buffer_pos = 0;
}

void AOFManager::start() {
  if (AOFManager::_instance == nullptr) {
    AOFManager::_instance = new AOFManager();
  } else {
    throw MAKE_EXCEPTION("AOFManager::start started twice.");
  }
}

void AOFManager::stop() {
  _instance->flush();
  delete AOFManager::_instance;
  AOFManager::_instance = nullptr;
}

AOFManager *AOFManager::instance() {
  return AOFManager::_instance;
}

void AOFManager::create_new() {
  TIMECODE_METRICS(ctm, "create", "AOFManager::create_new");
  _aof = nullptr;
  if (Options::instance()->strategy != STRATEGY::FAST_WRITE) {
    drop_old_if_needed();
  }
  _aof = AOFile_Ptr{new AOFile()};
}

void AOFManager::drop_closed_files(size_t count) {
  if (_down != nullptr) {
    auto closed = this->closed_aofs();

    TIMECODE_METRICS(ctmd, "drop", "AOFManager::drop_closed_files");
    size_t to_drop = std::min(closed.size(), count);
    for (size_t i = 0; i < to_drop; ++i) {
      auto f = closed.front();
      closed.pop_front();
      auto without_path = utils::fs::extract_filename(f);
      if (_files_send_to_drop.find(without_path) == _files_send_to_drop.end()) {
        this->drop_aof(f, _down);
      }
    }
    // clean set of sended to drop files.
    auto aofs_exists = Manifest::instance()->aof_list();
    std::set<std::string> aof_exists_set{aofs_exists.begin(), aofs_exists.end()};
    std::set<std::string> new_sended_files;
    for (auto &v : _files_send_to_drop) {
      if (aof_exists_set.find(v) != aof_exists_set.end()) {
        new_sended_files.insert(v);
      }
    }
    _files_send_to_drop = new_sended_files;
  }
}

void AOFManager::drop_old_if_needed() {
  auto closed = this->closed_aofs();
  drop_closed_files(closed.size());
}

std::list<std::string> AOFManager::aof_files() const {
  std::list<std::string> res;
  auto files = Manifest::instance()->aof_list();
  for (auto f : files) {
    auto full_path = utils::fs::append_path(Options::instance()->path, f);
    res.push_back(full_path);
  }
  return res;
}

std::list<std::string> AOFManager::closed_aofs() {
  auto all_files = aof_files();
  std::list<std::string> result;
  for (auto fn : all_files) {
    if (_aof == nullptr) {
      result.push_back(fn);
    } else {
      if (fn != this->_aof->filename()) {
        result.push_back(fn);
      }
    }
  }
  return result;
}

void AOFManager::drop_aof(const std::string &fname, IAofDropper *storage) {
  AOFile_Ptr ptr = AOFile_Ptr{new AOFile{fname, false}};
  auto without_path = utils::fs::extract_filename(fname);
  _files_send_to_drop.insert(without_path);
  storage->drop_aof(without_path);
}

void AOFManager::set_downlevel(IAofDropper *down) {
  _down = down;
  this->drop_old_if_needed();
}

dariadb::Time AOFManager::minTime() {
  std::lock_guard<std::mutex> lg(_locker);
  auto files = aof_files();
  dariadb::Time result = dariadb::MAX_TIME;
  for (auto filename : files) {
    AOFile aof(filename, true);
    auto local = aof.minTime();
    result = std::min(local, result);
  }
  size_t pos = 0;
  for (auto v : _buffer) {
    result = std::min(v.time, result);
    ++pos;
    if (pos > _buffer_pos) {
      break;
    }
  }
  return result;
}

dariadb::Time AOFManager::maxTime() {
  std::lock_guard<std::mutex> lg(_locker);
  auto files = aof_files();
  dariadb::Time result = dariadb::MIN_TIME;
  for (auto filename : files) {
    AOFile aof(filename, true);
    auto local = aof.maxTime();
    result = std::max(local, result);
  }
  for (auto v : _buffer) {
    result = std::max(v.time, result);
  }
  return result;
}

bool AOFManager::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                            dariadb::Time *maxResult) {
  TIMECODE_METRICS(ctmd, "minMaxTime", "AOFManager::minMaxTime");
  std::lock_guard<std::mutex> lg(_locker);
  auto files = aof_files();
  using MMRes = std::tuple<bool, dariadb::Time, dariadb::Time>;
  std::vector<MMRes> results{files.size()};
  std::vector<TaskResult_Ptr> task_res{files.size()};
  size_t num = 0;

  for (auto filename : files) {
    AsyncTask at = [filename, &results, num, id](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::DISK_IO, ti.kind);
      AOFile aof(filename, true);
      dariadb::Time lmin = dariadb::MAX_TIME, lmax = dariadb::MIN_TIME;
      if (aof.minMaxTime(id, &lmin, &lmax)) {
        results[num] = MMRes(true, lmin, lmax);
      } else {
        results[num] = MMRes(false, lmin, lmax);
      }
    };
    task_res[num] =
        ThreadManager::instance()->post(THREAD_COMMON_KINDS::DISK_IO, AT(at));
    num++;
  }

  for (auto &tw : task_res) {
    tw->wait();
  }

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

void AOFManager::foreach (const QueryInterval &q, IReaderClb * clbk) {
  TIMECODE_METRICS(ctmd, "foreach", "AOFManager::foreach");
  std::lock_guard<std::mutex> lg(_locker);
  auto files = aof_files();
  if (files.empty()) {
    return;
  }

  std::vector<TaskResult_Ptr> task_res{files.size()};
  size_t num = 0;

  for (auto filename : files) {
    AsyncTask at = [filename, &q, &clbk](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::DISK_IO, ti.kind);
      AOFile aof(filename, true);
      aof.foreach (q, clbk);
    };
    task_res[num] =
        ThreadManager::instance()->post(THREAD_COMMON_KINDS::DISK_IO, AT(at));
    num++;
  }
  for (auto &t : task_res) {
    t->wait();
  }

  size_t pos = 0;
  for (auto v : _buffer) {
    if (pos >= _buffer_pos) {
      break;
    }
    if (v.inQuery(q.ids, q.flag, q.from, q.to)) {
      clbk->call(v);
    }
    ++pos;
  }
}

Id2Meas AOFManager::readTimePoint(const QueryTimePoint &query) {
  TIMECODE_METRICS(ctmd, "readTimePoint", "AOFManager::readTimePoint");
  std::lock_guard<std::mutex> lg(_locker);
  auto files = aof_files();
  dariadb::Id2Meas sub_result;

  for (auto id : query.ids) {
    sub_result[id].flag = Flags::_NO_DATA;
    sub_result[id].time = query.time_point;
  }

  std::vector<Id2Meas> results{files.size()};
  std::vector<TaskResult_Ptr> task_res{files.size()};

  size_t num = 0;
  for (auto filename : files) {
    AsyncTask at = [filename, &query, num, &results](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::DISK_IO, ti.kind);
      AOFile aof(filename, true);
      results[num] = aof.readTimePoint(query);
    };
    task_res[num] =
        ThreadManager::instance()->post(THREAD_COMMON_KINDS::DISK_IO, AT(at));
    num++;
  }

  for (auto &tw : task_res) {
    tw->wait();
  }

  for (auto &out : results) {
    for (auto &kv : out) {
      auto it = sub_result.find(kv.first);
      if (it == sub_result.end()) {
        sub_result.insert(std::make_pair(kv.first, kv.second));
      } else {
        if (it->second.flag == Flags::_NO_DATA) {
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
        sub_result.insert(std::make_pair(v.id, v));
      } else {
        if ((v.time > it->second.time) && (v.time <= query.time_point)) {
          sub_result[v.id] = v;
        }
      }
    }
    ++pos;
    if (pos > _buffer_pos) {
      break;
    }
  }

  return sub_result;
}

Id2Meas AOFManager::currentValue(const IdArray &ids, const Flag &flag) {
  auto files = aof_files();

  dariadb::Id2Meas meases;
  for (const auto &f : files) {
    AOFile c(f, true);
    auto sub_rdr = c.currentValue(ids, flag);

    for (auto &kv : sub_rdr) {
      auto it = meases.find(kv.first);
      if (it == meases.end()) {
        meases.insert(std::make_pair(kv.first, kv.second));
      } else {
        if ((it->second.flag == Flags::_NO_DATA) || (it->second.time<kv.second.time)) {
          meases[kv.first] = kv.second;
        }
      }
    }
  }
  return meases;
}

dariadb::append_result AOFManager::append(const Meas &value) {
  TIMECODE_METRICS(ctmd, "append", "AOFManager::append");
  std::lock_guard<std::mutex> lg(_locker);
  _buffer[_buffer_pos] = value;
  _buffer_pos++;

  if (_buffer_pos >= Options::instance()->aof_buffer_size) {
    flush_buffer();
  }
  return dariadb::append_result(1, 0);
}

void AOFManager::flush_buffer() {
  if (_buffer_pos == size_t(0)) {
    return;
  }
  AsyncTask at = [this](const ThreadInfo &ti) {
	  TKIND_CHECK(THREAD_COMMON_KINDS::DISK_IO, ti.kind);
	  if (_aof == nullptr) {
		  create_new();
	  }
	  size_t pos = 0;
	  size_t total_writed = 0;
	  while (1) {
		  auto res = _aof->append(_buffer.begin() + pos, _buffer.begin() + _buffer_pos);
		  total_writed += res.writed;
		  if (total_writed != _buffer_pos) {
			  create_new();
			  pos += res.writed;
		  }
		  else {
			  break;
		  }
	  }
	  _buffer_pos = 0;
  };
  auto async_r = ThreadManager::instance()->post(THREAD_COMMON_KINDS::DISK_IO, AT(at));
  async_r->wait();
}

void AOFManager::flush() {
  TIMECODE_METRICS(ctmd, "flush", "AOFManager::flush");
  flush_buffer();
}

size_t AOFManager::files_count() const {
  return aof_files().size();
}

void AOFManager::erase(const std::string &fname) {
  Manifest::instance()->aof_rm(fname);
  utils::fs::rm(utils::fs::append_path(Options::instance()->path, fname));
}
