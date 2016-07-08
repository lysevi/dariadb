#include "capacitor_manager.h"
#include "../flags.h"
#include "../utils/exception.h"
#include "../utils/fs.h"
#include "../utils/metrics.h"
#include "inner_readers.h"
#include "manifest.h"
#include "../utils/thread_manager.h"
#include <cassert>

using namespace dariadb::storage;
using namespace dariadb;
using namespace dariadb::utils::async;

CapacitorManager *CapacitorManager::_instance = nullptr;

CapacitorManager::~CapacitorManager() {}

CapacitorManager::CapacitorManager(const Params &param) : _params(param) {
  _down = nullptr;
 /* if (!dariadb::utils::fs::path_exists(_params.path)) {
    dariadb::utils::fs::mkdir(_params.path);
  }*/

  auto files = cap_files();
  for (auto f : files) {
    auto hdr = Capacitor::readHeader(f);
    if (!hdr.is_full) {
      auto p = Capacitor::Params(_params.B, _params.path);
      _cap = Capacitor_Ptr{new Capacitor(p, f)};
      break;
    }
  }
  /*if (_cap == nullptr) {
    create_new();
  }*/
}

void CapacitorManager::fsck(bool force_check) {
	auto files = cap_files();
	for (auto f : files) {
		auto hdr = Capacitor::readHeader(f);
		if (force_check || (!hdr.is_closed && hdr.is_open_to_write)) {
			auto p = Capacitor::Params(_params.B, _params.path);
			auto c = Capacitor_Ptr{ new Capacitor(p, f) };
			c->fsck();
		}
	}
}

void CapacitorManager::start(const Params &param) {
  if (CapacitorManager::_instance == nullptr) {
    CapacitorManager::_instance = new CapacitorManager(param);
  } else {
    throw MAKE_EXCEPTION("CapacitorManager::start started twice.");
  }
}

void CapacitorManager::stop() {
  delete CapacitorManager::_instance;
  CapacitorManager::_instance = nullptr;
}

CapacitorManager *dariadb::storage::CapacitorManager::instance() {
  return CapacitorManager::_instance;
}

Capacitor_Ptr CapacitorManager::create_new(std::string filename){
    TIMECODE_METRICS(ctm, "create", "CapacitorManager::create_new");
    _cap = nullptr;
    auto p = Capacitor::Params(_params.B, _params.path);
    if (_params.max_levels != 0) {
      p.max_levels = _params.max_levels;
    }
    if (_down != nullptr) {
        auto closed = this->closed_caps();

        if (closed.size() > _params.max_closed_caps && _params.max_closed_caps>0) {
          size_t to_drop = closed.size() / 4;
          drop_part_unsafe(to_drop);
        }
    }
   return Capacitor_Ptr{new Capacitor(p, filename)};
}

Capacitor_Ptr CapacitorManager::create_new() {
  return create_new(Capacitor::file_name());
}

std::list<std::string> CapacitorManager::cap_files() const {
  std::list<std::string> res;
  auto files = Manifest::instance()->cola_list();
  for (auto f : files) {
    auto full_path = utils::fs::append_path(_params.path, f);
    res.push_back(full_path);
  }
  return res;
}

std::list<std::string>
CapacitorManager::caps_by_filter(std::function<bool(const Capacitor::Header &)> pred) {
  std::list<std::string> result;
  auto names = cap_files();
  for (auto file_name : names) {
    auto hdr = Capacitor::readHeader(file_name);
    if (pred(hdr)) {
      result.push_back(file_name);
    }
  }
  return result;
}

std::list<std::string> CapacitorManager::closed_caps() {
  auto pred = [](const Capacitor::Header &hdr) { return hdr.is_full; };

  auto files = caps_by_filter(pred);
  return files;
}

void dariadb::storage::CapacitorManager::drop_cap(const std::string &fname) {
	auto without_path = utils::fs::extract_filename(fname);
	_files_send_to_drop.insert(without_path);
  _down->drop(fname);
}

void CapacitorManager::drop_part_unsafe(size_t count) {
    TIMECODE_METRICS(ctmd, "drop", "CapacitorManager::drop_part");
    auto closed = this->closed_caps();
	using File2Header = std::tuple<std::string, Capacitor::Header>;
	std::vector<File2Header> file2headers{ closed.size() };

	size_t pos = 0;
	for (auto f : closed) {
		auto without_path = utils::fs::extract_filename(f);
		if (_files_send_to_drop.find(without_path) == _files_send_to_drop.end()) {
			auto cheader = Capacitor::readHeader(f);
			file2headers[pos] = std::tie(f, cheader);
			++pos;
		}
	}
	std::sort(file2headers.begin(), file2headers.begin()+pos, 
		[](const File2Header&l, const File2Header&r) {
		return std::get<1>(l).minTime < std::get<1>(r).minTime;
	});
    auto drop_count = std::min(pos, count);
	
    for (size_t i = 0; i < drop_count; ++i) {
		std::string f = std::get<0>(file2headers[i]);
      this->drop_cap(f);
    }

	//clean set of sended to drop files.
	auto caps_exists = Manifest::instance()->cola_list();
	std::set<std::string> caps_exists_set{ caps_exists.begin(),caps_exists.end() };
	std::set<std::string> new_sended_files;
	for (auto&v : _files_send_to_drop) {
		if (caps_exists_set.find(v) != caps_exists_set.end()) {
			new_sended_files.insert(v);
		}
	}
	_files_send_to_drop = new_sended_files;
}

void CapacitorManager::drop_part(size_t count) {
  std::lock_guard<std::mutex> lg(_locker);
  drop_part_unsafe(count);
}

dariadb::Time CapacitorManager::minTime() {
  std::lock_guard<std::mutex> lg(_locker);
  auto files = cap_files();
  dariadb::Time result = dariadb::MAX_TIME;
  for (auto filename : files) {
    auto local = Capacitor::readHeader(filename).minTime;
    result = std::min(local, result);
  }
  return result;
}

dariadb::Time CapacitorManager::maxTime() {
  std::lock_guard<std::mutex> lg(_locker);
  auto files = cap_files();
  dariadb::Time result = dariadb::MIN_TIME;
  for (auto filename : files) {
    auto local = Capacitor::readHeader(filename).maxTime;
    result = std::max(local, result);
  }
  return result;
}

bool CapacitorManager::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                                  dariadb::Time *maxResult) {
  TIMECODE_METRICS(ctmd, "minMaxTime", "CapacitorManager::minMaxTime");
  std::lock_guard<std::mutex> lg(_locker);
  auto files = cap_files();
  auto p = Capacitor::Params(_params.B, _params.path);



  using MMRes=std::tuple<bool,dariadb::Time, dariadb::Time>;
  std::vector<MMRes> results{files.size()};
  std::vector<TaskResult_Ptr> task_res{files.size()};
  size_t num=0;

  for (auto filename : files) {
      AsyncTask at=[filename, &results, num, &p, id](const ThreadInfo&ti){
          TKIND_CHECK(THREAD_COMMON_KINDS::FILE_READ, ti.kind);
          auto raw = new Capacitor(p, filename, true);
          Capacitor_Ptr cptr{raw};
          dariadb::Time lmin = dariadb::MAX_TIME, lmax = dariadb::MIN_TIME;
          if (cptr->minMaxTime(id, &lmin, &lmax)){
              results[num]=MMRes(true,lmin,lmax);
          }else{
              results[num]=MMRes(false,lmin,lmax);
          }
      };
      task_res[num]=ThreadManager::instance()->post(THREAD_COMMON_KINDS::FILE_READ, AT(at));
      num++;
  }


  for(auto&tw:task_res){
      tw->wait();
  }

  bool res = false;

  *minResult = dariadb::MAX_TIME;
  *maxResult = dariadb::MIN_TIME;
  for(auto&subRes:results){
      if (std::get<0>(subRes)) {
            res = true;
            *minResult = std::min(std::get<1>(subRes), *minResult);
            *maxResult = std::max(std::get<2>(subRes), *maxResult);
          }
  }

  return res;
}

Reader_ptr CapacitorManager::readInterval(const QueryInterval &query) {
  TIMECODE_METRICS(ctmd, "readInterval", "CapacitorManager::readInterval");
  std::lock_guard<std::mutex> lg(_locker);
  auto pred = [query](const Capacitor::Header &hdr) {

    bool flag_exists = hdr.check_flag(query.flag);
    if (!flag_exists) {
      return false;
    }

    auto interval_check((hdr.minTime >= query.from && hdr.maxTime <= query.to) ||
                        (utils::inInterval(query.from, query.to, hdr.minTime)) ||
                        (utils::inInterval(query.from, query.to, hdr.maxTime)) ||
                        (utils::inInterval(hdr.minTime, hdr.maxTime, query.from)) ||
                        (utils::inInterval(hdr.minTime, hdr.maxTime, query.to)));

    if (!interval_check) {
      return false;
    }
    if (!hdr.check_id(query.ids)) {
      return false;
    } else {
      return true;
    }
  };

  auto files = caps_by_filter(pred);
  auto p = Capacitor::Params(_params.B, _params.path);
  TP_Reader *raw = new TP_Reader;
  std::map<dariadb::Id, std::set<Meas, meas_time_compare_less>> sub_result;

  std::vector<Meas::MeasList> results{files.size()};
  std::vector<TaskResult_Ptr> task_res{files.size()};
  size_t num=0;
  for (auto filename : files) {
      AsyncTask at=[filename,&query, &results, num,&p](const ThreadInfo&ti){
          TKIND_CHECK(THREAD_COMMON_KINDS::FILE_READ, ti.kind);
          auto raw_cap = new Capacitor(p, filename, true);
          raw_cap->readInterval(query)->readAll(&results[num]);
          delete raw_cap;
      };
      task_res[num]=ThreadManager::instance()->post(THREAD_COMMON_KINDS::FILE_READ, AT(at));
      num++;
  }

  for(auto&tw:task_res){
      tw->wait();
  }

  for(auto&out:results){
      for (auto m : out) {
          if (m.flag == Flags::_NO_DATA) {
              continue;
          }
          sub_result[m.id].insert(m);
      }
  }

  for (auto &kv : sub_result) {
    raw->_ids.push_back(kv.first);
    for (auto &m : kv.second) {
      raw->_values.push_back(m);
    }
  }
  raw->reset();
  return Reader_ptr(raw);
}

Reader_ptr CapacitorManager::readInTimePoint(const QueryTimePoint &query) {
  TIMECODE_METRICS(ctmd, "readInTimePoint", "CapacitorManager::readInTimePoint");
  std::lock_guard<std::mutex> lg(_locker);
  auto pred = [query](const Capacitor::Header &hdr) {
    if (!hdr.check_flag(query.flag)) {
      return false;
    }

    auto interval_check = hdr.maxTime < query.time_point;
    if (!interval_check) {
      return false;
    }

    if (!hdr.check_id(query.ids)) {
      return false;
    }
    return true;
  };

  auto files = caps_by_filter(pred);
  auto p = Capacitor::Params(_params.B, _params.path);
  TP_Reader *raw = new TP_Reader;
  dariadb::Meas::Id2Meas sub_result;

  for (auto id : query.ids) {
    sub_result[id].flag = Flags::_NO_DATA;
    sub_result[id].time = query.time_point;
  }

  std::vector<Meas::MeasList> results{files.size()};
  std::vector<TaskResult_Ptr> task_res{files.size()};

  size_t num=0;
  for (auto filename : files) {
      AsyncTask at=[filename,&p,&query,num,&results](const ThreadInfo&ti){
          TKIND_CHECK(THREAD_COMMON_KINDS::FILE_READ, ti.kind);
          auto raw_cap = new Capacitor(p, filename, true);
          raw_cap->readInTimePoint(query)->readAll(&results[num]);
          delete raw_cap;
      };
      task_res[num]=ThreadManager::instance()->post(THREAD_COMMON_KINDS::FILE_READ, AT(at));
      num++;
  }

  for(auto&tw:task_res){
      tw->wait();
  }

  for(auto&out:results){
      for (auto &m : out) {
          if (sub_result[m.id].flag == Flags::_NO_DATA) {
              sub_result[m.id] = m;
          }
      }
  }

  for (auto &kv : sub_result) {
    raw->_ids.push_back(kv.first);
    raw->_values.push_back(kv.second);
  }
  raw->reset();
  return Reader_ptr(raw);
}

Reader_ptr CapacitorManager::currentValue(const IdArray &ids, const Flag &flag) {
  TP_Reader *raw = new TP_Reader;
  auto files = cap_files();

  auto p = Capacitor::Params(_params.B, _params.path);
  dariadb::Meas::Id2Meas meases;
  for (const auto &f : files) {
    auto c = Capacitor_Ptr{new Capacitor(p, f, true)};
    auto sub_rdr = c->currentValue(ids, flag);
    Meas::MeasList out;
    sub_rdr->readAll(&out);

    for (auto &m : out) {
      auto it = meases.find(m.id);
      if (it == meases.end()) {
        meases.insert(std::make_pair(m.id, m));
      } else {
        if (it->second.flag == Flags::_NO_DATA) {
          meases[m.id] = m;
        }
      }
    }
  }
  for (auto &kv : meases) {
    raw->_values.push_back(kv.second);
    raw->_ids.push_back(kv.first);
  }
  raw->reset();
  return Reader_ptr(raw);
}

void CapacitorManager::append(std::string filename,const Meas::MeasArray& ma){
    TIMECODE_METRICS(ctmd, "append", "CapacitorManager::append(std::string filename)");
    std::lock_guard<std::mutex> lg(_locker);

    auto target=create_new(filename);
    for(auto v:ma){
        if(target->append(v).writed==0){
            throw MAKE_EXCEPTION("target size to small.");
        }
    }
	target->close();
    target=nullptr;
}

dariadb::append_result CapacitorManager::append(const Meas &value) {
  TIMECODE_METRICS(ctmd, "append", "CapacitorManager::append");
  std::lock_guard<std::mutex> lg(_locker);
  if (_cap == nullptr) {
    _cap=create_new();
  }
  auto res = _cap->append(value);
  if (res.writed != 1) {
    _cap=create_new();
    return _cap->append(value);
  } else {
    return res;
  }
}

void CapacitorManager::flush() {
	TIMECODE_METRICS(ctmd, "flush", "CapacitorManager::flush");
}

size_t CapacitorManager::files_count() const {
  return cap_files().size();
}
