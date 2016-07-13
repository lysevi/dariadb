#include "aof_manager.h"
#include "../flags.h"
#include "../utils/exception.h"
#include "../utils/fs.h"
#include "../utils/logger.h"
#include "../utils/metrics.h"
#include "../utils/thread_manager.h"
#include "manifest.h"
#include <cassert>
#include <iterator>
#include <tuple>

using namespace dariadb::storage;
using namespace dariadb;
using namespace dariadb::utils::async;

AOFManager *AOFManager::_instance = nullptr;

AOFManager::~AOFManager() {
  this->flush();
}

AOFManager::AOFManager(const Params &param) : _params(param) {
  _down = nullptr;
  
  if (dariadb::utils::fs::path_exists(_params.path)) {
    auto aofs = Manifest::instance()->aof_list();
    for (auto f : aofs) {
      auto full_filename = utils::fs::append_path(param.path, f);
      if (AOFile::writed(full_filename) != param.max_size) {
        logger_info("AofManager: open exist file " << f);
        AOFile::Params params(_params.max_size, param.path);
        AOFile_Ptr p{new AOFile(params, full_filename)};
        _aof = p;
		break;
	  }
    }
  }
  
  _buffer.resize(_params.buffer_size);
  _buffer_pos = 0;
}

void AOFManager::start(const Params &param) {
  if (AOFManager::_instance == nullptr) {
    AOFManager::_instance = new AOFManager(param);
  } else {
    throw MAKE_EXCEPTION("AOFManager::start started twice.");
  }
}

void AOFManager::stop() {
  _instance->flush();
  delete AOFManager::_instance;
  AOFManager::_instance = nullptr;
}

AOFManager *dariadb::storage::AOFManager::instance() {
  return AOFManager::_instance;
}

void AOFManager::create_new() {
  TIMECODE_METRICS(ctm, "create", "AOFManager::create_new");
  _aof = nullptr;
  auto p = AOFile::Params(_params.max_size, _params.path);
  drop_old_if_needed();
  _aof = AOFile_Ptr{new AOFile(p)};
}

void AOFManager::drop_old_if_needed() {
	if (_down != nullptr) {
		auto closed = this->closed_aofs();
		// if (closed.size() > _params.max_closed_aofs)
		{
			TIMECODE_METRICS(ctmd, "drop", "AOFManager::create_new::dump");
			size_t to_drop = closed.size();
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
			std::set<std::string> aof_exists_set{ aofs_exists.begin(), aofs_exists.end() };
			std::set<std::string> new_sended_files;
			for (auto &v : _files_send_to_drop) {
				if (aof_exists_set.find(v) != aof_exists_set.end()) {
					new_sended_files.insert(v);
				}
			}
			_files_send_to_drop = new_sended_files;
		}
	}
}

std::list<std::string> AOFManager::aof_files() const {
  std::list<std::string> res;
  auto files = Manifest::instance()->aof_list();
  for (auto f : files) {
    auto full_path = utils::fs::append_path(_params.path, f);
    res.push_back(full_path);
  }
  return res;
}

std::list<std::string> dariadb::storage::AOFManager::closed_aofs() {
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

void dariadb::storage::AOFManager::drop_aof(const std::string &fname,
                                            AofFileDropper *storage) {
  auto p = AOFile::Params(_params.max_size, _params.path);
  AOFile_Ptr ptr = AOFile_Ptr{new AOFile{p, fname, false}};
  auto without_path = utils::fs::extract_filename(fname);
  _files_send_to_drop.insert(without_path);
  storage->drop(ptr, without_path);
}

void AOFManager::set_downlevel(AofFileDropper *down) {
	_down = down;
	this->drop_old_if_needed();
}

dariadb::Time AOFManager::minTime() {
  std::lock_guard<std::mutex> lg(_locker);
  auto files = aof_files();
  dariadb::Time result = dariadb::MAX_TIME;
  for (auto filename : files) {
    auto p = AOFile::Params(_params.max_size, _params.path);
    AOFile aof(p, filename, true);
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
    auto p = AOFile::Params(_params.max_size, _params.path);
    AOFile aof(p, filename, true);
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
  auto p = AOFile::Params(_params.max_size, _params.path);

  using MMRes = std::tuple<bool, dariadb::Time, dariadb::Time>;
  std::vector<MMRes> results{files.size()};
  std::vector<TaskResult_Ptr> task_res{files.size()};
  size_t num = 0;

  for (auto filename : files) {
    AsyncTask at = [filename, &results, num, &p, id](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::FILE_READ, ti.kind);
      AOFile aof(p, filename, true);
      dariadb::Time lmin = dariadb::MAX_TIME, lmax = dariadb::MIN_TIME;
      if (aof.minMaxTime(id, &lmin, &lmax)) {
        results[num] = MMRes(true, lmin, lmax);
      } else {
        results[num] = MMRes(false, lmin, lmax);
      }
    };
    task_res[num] =
        ThreadManager::instance()->post(THREAD_COMMON_KINDS::FILE_READ, AT(at));
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

void AOFManager::foreach(const QueryInterval&q, ReaderClb*clbk) {
	TIMECODE_METRICS(ctmd, "foreach", "AOFManager::foreach");
	std::lock_guard<std::mutex> lg(_locker);
	auto files = aof_files();
	if (files.empty()) {
		return;
	}
	auto p = AOFile::Params(_params.max_size, _params.path);

	std::vector<TaskResult_Ptr> task_res{ files.size() };
	size_t num = 0;

	for (auto filename : files) {
		AsyncTask at = [filename, &q, &clbk, &p](const ThreadInfo &ti) {
			TKIND_CHECK(THREAD_COMMON_KINDS::FILE_READ, ti.kind);
			AOFile aof(p, filename, true);
			aof.foreach(q, clbk);
		};
		task_res[num] =
			ThreadManager::instance()->post(THREAD_COMMON_KINDS::FILE_READ, AT(at));
		num++;
	}
    for(auto&t:task_res){
        t->wait();
    }

    size_t pos = 0;
    for (auto v : _buffer) {
      if (pos >= _buffer_pos) {
        break;
      }
      if (v.inQuery(q.ids, q.flag, q.source, q.from, q.to)) {
        clbk->call(v);
      }
      ++pos;
    }
}

Meas::MeasList AOFManager::readInterval(const QueryInterval &query) {
  TIMECODE_METRICS(ctmd, "readInterval", "AOFManager::readInterval");
  Meas::MeasList result;
  
  std::map<dariadb::Id, std::set<Meas, meas_time_compare_less>> sub_result;

  std::unique_ptr<MList_ReaderClb> clbk{ new MList_ReaderClb };
  this->foreach(query, clbk.get());

  for (auto m : clbk->mlist) {
     if (m.flag == Flags::_NO_DATA) {
       continue;
     }
	sub_result[m.id].insert(m);
  }


  for (auto id : query.ids) {
	  auto sublist = sub_result.find(id);
	  if (sublist == sub_result.end()) {
		  continue;
	  }
	  for (auto v : sublist->second) {
		  result.push_back(v);
	  }
  }
  return result;
}

Meas::Id2Meas AOFManager::readInTimePoint(const QueryTimePoint &query) {
  TIMECODE_METRICS(ctmd, "readInTimePoint", "AOFManager::readInTimePoint");
  std::lock_guard<std::mutex> lg(_locker);
  auto files = aof_files();
  auto p = AOFile::Params(_params.max_size, _params.path);
  dariadb::Meas::Id2Meas sub_result;

  for (auto id : query.ids) {
    sub_result[id].flag = Flags::_NO_DATA;
    sub_result[id].time = query.time_point;
  }

  std::vector<Meas::Id2Meas> results{files.size()};
  std::vector<TaskResult_Ptr> task_res{files.size()};

  size_t num = 0;
  for (auto filename : files) {
    AsyncTask at = [filename, &p, &query, num, &results](const ThreadInfo &ti) {
      TKIND_CHECK(THREAD_COMMON_KINDS::FILE_READ, ti.kind);
      AOFile aof(p, filename, true);
	  results[num] = aof.readInTimePoint(query);
    };
    task_res[num] =
        ThreadManager::instance()->post(THREAD_COMMON_KINDS::FILE_READ, AT(at));
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
    if (v.inQuery(query.ids, query.flag, query.source)) {
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

Meas::Id2Meas AOFManager::currentValue(const IdArray &ids, const Flag &flag) {
  auto files = aof_files();

  auto p = AOFile::Params(_params.max_size, _params.path);
  dariadb::Meas::Id2Meas meases;
  for (const auto &f : files) {
    AOFile c(p, f, true);
    auto sub_rdr = c.currentValue(ids, flag);

    for (auto &kv : sub_rdr) {
      auto it = meases.find(kv.first);
      if (it == meases.end()) {
        meases.insert(std::make_pair(kv.first, kv.second));
      } else {
        if (it->second.flag == Flags::_NO_DATA) {
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

  if (_buffer_pos >= _params.buffer_size) {
    flush_buffer();
  }
  return dariadb::append_result(1, 0);
}

void AOFManager::flush_buffer() {
    if(_buffer_pos==size_t(0)){
        return;
    }
  if (_aof == nullptr) {
    create_new();
  }
  size_t pos=0;
  size_t total_writed=0;
  while (1) {
    auto res = _aof->append(_buffer.begin()+pos, _buffer.begin() + _buffer_pos);
    total_writed+=res.writed;
    if (total_writed != _buffer_pos) {
      create_new();
     pos+=res.writed;
    } else {
      break;
    }
  }
  _buffer_pos = 0;
}

void AOFManager::flush() {
  TIMECODE_METRICS(ctmd, "flush", "AOFManager::flush");
  flush_buffer();
}

size_t AOFManager::files_count() const {
  return aof_files().size();
}
