#include "aof_manager.h"
#include "manifest.h"
#include "../utils/exception.h"
#include "../utils/fs.h"
#include "../flags.h"
#include "inner_readers.h"
#include <cassert>

using namespace dariadb::storage;
using namespace dariadb;

AOFManager *AOFManager::_instance = nullptr;

AOFManager::~AOFManager(){
}

AOFManager::AOFManager(const Params & param):_params(param){
	_down = nullptr;
    if (!dariadb::utils::fs::path_exists(_params.path)) {
      dariadb::utils::fs::mkdir(_params.path);
    }
}

void AOFManager::start(const Params & param){
    if (AOFManager::_instance == nullptr) {
        AOFManager::_instance = new AOFManager(param);
	}
	else {
        throw MAKE_EXCEPTION("AOFManager::start started twice.");
	}
}

void AOFManager::stop(){
    delete AOFManager::_instance;
    AOFManager::_instance = nullptr;
}

AOFManager * dariadb::storage::AOFManager::instance(){
    return AOFManager::_instance;
}

void AOFManager::create_new(){
//    _cap=nullptr;
//     auto p=AOFManager::Params(_params.max_size,_params.path);
//     if(_params.max_levels!=0){
//         p.max_levels=_params.max_levels;
//     }
//     if(_down!=nullptr){
//         auto closed=this->closed_caps();
//		 const size_t MAX_CLOSED_CAPS = 10;
//         if(closed.size()>MAX_CLOSED_CAPS){
//             size_t to_drop=closed.size()/2;
//             for(size_t i=0;i<to_drop;++i){
//                 auto f=closed.front();
//                 closed.pop_front();
//                 this->drop_cap(f,_down);
//             }
//         }
//     }
//    _cap=Capacitor_Ptr{new Capacitor(p)};
}

std::list<std::string> AOFManager::aof_files() const{
	std::list<std::string> res;
	auto files = Manifest::instance()->aof_list();
	for (auto f : files) {
		auto full_path = utils::fs::append_path(_params.path, f);
		res.push_back(full_path);
	}
	return res;
}

void dariadb::storage::AOFManager::drop_aof(const std::string & fname, MeasWriter* storage){
    //boost::upgrade_lock<boost::shared_mutex> lg(_locker);

//	auto p = Capacitor::Params(_params.B, _params.path);
//	auto cap = Capacitor_Ptr{ new Capacitor{p,fname, false} };
//	cap->drop_to_stor(storage);
//	cap = nullptr;
//	utils::fs::rm(fname);
//	auto without_path = utils::fs::extract_filename(fname);
//	Manifest::instance()->cola_rm(without_path);
}


dariadb::Time AOFManager::minTime(){
    std::lock_guard<std::mutex> lg(_locker);
    //auto files = cap_files();
	dariadb::Time result = dariadb::MAX_TIME;
//	for (auto filename : files) {
//		auto local = Capacitor::readHeader(filename).minTime;
//		result = std::min(local, result);
//	}
	return result;
}

dariadb::Time AOFManager::maxTime(){
    std::lock_guard<std::mutex> lg(_locker);
//	auto files = cap_files();
	dariadb::Time result = dariadb::MIN_TIME;
//	for (auto filename : files) {
//		auto local = Capacitor::readHeader(filename).maxTime;
//		result = std::max(local, result);
//	}
	return result;
}

bool AOFManager::minMaxTime(dariadb::Id id, dariadb::Time * minResult, dariadb::Time * maxResult){
    std::lock_guard<std::mutex> lg(_locker);
//	auto files = cap_files();
//    auto p=Capacitor::Params(_params.B,_params.path);

    bool res=false;
    *minResult=dariadb::MAX_TIME;
    *maxResult=dariadb::MIN_TIME;

//    for(auto filename :files){
//        auto raw=new Capacitor(p,filename,true);
//        Capacitor_Ptr cptr{raw};
//        dariadb::Time lmin=dariadb::MAX_TIME, lmax=dariadb::MIN_TIME;
//        if(cptr->minMaxTime(id,&lmin,&lmax)){
//            res=true;
//            *minResult=std::min(lmin,*minResult);
//            *maxResult=std::max(lmax,*maxResult);
//        }
//    }
    return res;
}


Reader_ptr AOFManager::readInterval(const QueryInterval & query){
    std::lock_guard<std::mutex> lg(_locker);
//	auto pred = [query](const Capacitor::Header &hdr) {
//		auto interval_check((hdr.minTime >= query.from && hdr.maxTime <= query.to) ||
//			(utils::inInterval(query.from, query.to, hdr.minTime)) ||
//			(utils::inInterval(query.from, query.to, hdr.maxTime)) ||
//			(utils::inInterval(hdr.minTime, hdr.maxTime, query.from)) ||
//			(utils::inInterval(hdr.minTime, hdr.maxTime, query.to)));
//		return interval_check;
//	};

//	auto files = caps_by_filter(pred);
//	auto p = Capacitor::Params(_params.B, _params.path);
//	TP_Reader *raw = new TP_Reader;
//	std::map<dariadb::Id, std::set<Meas, meas_time_compare_less>> sub_result;

//	for (auto filename : files) {
//		auto raw = new Capacitor(p, filename, true);
//		Meas::MeasList out;
//		raw->readInterval(query)->readAll(&out);
//		for (auto m : out) {
//			//TODO check!
//			if (m.flag == Flags::_NO_DATA) {
//				continue;
//			}
//			sub_result[m.id].insert(m);
//		}
//		delete raw;
//	}

//	for (auto &kv : sub_result) {
//		raw->_ids.push_back(kv.first);
//		for (auto &m : kv.second) {
//			raw->_values.push_back(m);
//		}
//	}
//	raw->reset();
//	return Reader_ptr(raw);
    return nullptr;
}

Reader_ptr AOFManager::readInTimePoint(const QueryTimePoint & query){
    std::lock_guard<std::mutex> lg(_locker);
//	auto pred = [query](const Capacitor::Header &hdr) {
//		auto interval_check = hdr.maxTime < query.time_point;
//		//TODO check this.
//			//(utils::inInterval(hdr.minTime, hdr.maxTime, query.time_point));
//		return interval_check;
//	};

//	auto files = caps_by_filter(pred);
//	auto p = Capacitor::Params(_params.B, _params.path);
//	TP_Reader *raw = new TP_Reader;
//	dariadb::Meas::Id2Meas sub_result;

//	for (auto filename : files) {
//		auto raw = new Capacitor(p, filename, true);
//		Meas::MeasList out;
//		raw->readInTimePoint(query)->readAll(&out);

//		for (auto &m : out) {
//			auto it = sub_result.find(m.id);
//			if (it == sub_result.end()) {
//				sub_result.insert(std::make_pair(m.id, m));
//			}
//			else {
//				if (it->second.flag == Flags::_NO_DATA) {
//					sub_result[m.id] = m;
//				}
//			}
//		}

//		delete raw;
//	}

//	for (auto &kv : sub_result) {
//		raw->_ids.push_back(kv.first);
//		raw->_values.push_back(kv.second);
//	}
//	raw->reset();
//	return Reader_ptr(raw);
    return nullptr;
}

Reader_ptr AOFManager::currentValue(const IdArray & ids, const Flag & flag){
//	TP_Reader *raw = new TP_Reader;
//	auto files = cap_files();
	
//	auto p = Capacitor::Params(_params.B, _params.path);
//	dariadb::Meas::Id2Meas meases;
//	for (const auto &f : files) {
//		auto c = Capacitor_Ptr{ new Capacitor(p,f, true) };
//		auto sub_rdr=c->currentValue(ids, flag);
//		Meas::MeasList out;
//		sub_rdr->readAll(&out);

//		for (auto &m : out) {
//			auto it = meases.find(m.id);
//			if (it == meases.end()) {
//				meases.insert(std::make_pair(m.id, m));
//			}
//			else {
//				if (it->second.flag == Flags::_NO_DATA) {
//					meases[m.id] = m;
//				}
//			}
//		}
//	}
//	for (auto &kv : meases) {
//		raw->_values.push_back(kv.second);
//		raw->_ids.push_back(kv.first);
//	}
//	raw->reset();
//	return Reader_ptr(raw);
    return nullptr;
}

dariadb::append_result AOFManager::append(const Meas & value){
//    std::lock_guard<std::mutex> lg(_locker);
//    auto res=_cap->append(value);
//    if(res.writed!=1){
//        create_new();
//        return _cap->append(value);
//    }else{
//        return res;
//    }
    return dariadb::append_result(0,1);
}

void AOFManager::flush(){
}

void AOFManager::subscribe(const IdArray & ids, const Flag & flag, const ReaderClb_ptr & clbk){
	NOT_IMPLEMENTED;
}

size_t AOFManager::files_count() const {
    std::lock_guard<std::mutex> lg(_locker);
    return aof_files().size();
}
