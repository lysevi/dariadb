#include "capacitor_manager.h"
#include "manifest.h"
#include "../utils/exception.h"
#include "../utils/fs.h"
#include "../flags.h"
#include "inner_readers.h"
#include <cassert>

#include <boost/thread/locks.hpp>

using namespace dariadb::storage;
using namespace dariadb;

CapacitorManager *CapacitorManager::_instance = nullptr;

CapacitorManager::~CapacitorManager(){
}

CapacitorManager::CapacitorManager(const Params & param):_params(param){
    if (!dariadb::utils::fs::path_exists(_params.path)) {
      dariadb::utils::fs::mkdir(_params.path);
    }

	auto files = cap_files();
    for(auto f:files){
        auto hdr=Capacitor::readHeader(f);
        if(!hdr.is_full){
             auto p=Capacitor::Params(_params.B,_params.path);
            _cap=Capacitor_Ptr{new Capacitor(p,f)};
            break;
        }
    }
    if(_cap==nullptr){
        create_new();
    }
}

void CapacitorManager::start(const Params & param){
	if (CapacitorManager::_instance == nullptr) {
		CapacitorManager::_instance = new CapacitorManager(param);
	}
	else {
		throw MAKE_EXCEPTION("CapacitorManager::start started twice.");
	}
}

void CapacitorManager::stop(){
	delete CapacitorManager::_instance;
	CapacitorManager::_instance = nullptr;
}

CapacitorManager * dariadb::storage::CapacitorManager::instance(){
	return CapacitorManager::_instance;
}

void CapacitorManager::create_new(){
    _cap=nullptr;
     auto p=Capacitor::Params(_params.B,_params.path);
     if(_params.max_levels!=0){
         p.max_levels=_params.max_levels;
     }
    _cap=Capacitor_Ptr{new Capacitor(p)};
}

std::list<std::string> CapacitorManager::cap_files() const
{
	std::list<std::string> res;
	auto files = Manifest::instance()->cola_list();
	for (auto f : files) {
		auto full_path = utils::fs::append_path(_params.path, f);
		res.push_back(full_path);
	}
	return res;
}

dariadb::Time CapacitorManager::minTime(){
	boost::shared_lock<boost::shared_mutex> lg(_locker);
	auto files = cap_files();
	dariadb::Time result = dariadb::MAX_TIME;
	for (auto filename : files) {
		auto local = Capacitor::readHeader(filename).minTime;
		result = std::min(local, result);
	}
	return result;
}

dariadb::Time CapacitorManager::maxTime(){
	boost::shared_lock<boost::shared_mutex> lg(_locker);
	auto files = cap_files();
	dariadb::Time result = dariadb::MIN_TIME;
	for (auto filename : files) {
		auto local = Capacitor::readHeader(filename).maxTime;
		result = std::max(local, result);
	}
	return result;
}

bool CapacitorManager::minMaxTime(dariadb::Id id, dariadb::Time * minResult, dariadb::Time * maxResult){
    boost::shared_lock<boost::shared_mutex> lg(_locker);
	auto files = cap_files();
    auto p=Capacitor::Params(_params.B,_params.path);

    bool res=false;
    *minResult=dariadb::MAX_TIME;
    *maxResult=dariadb::MIN_TIME;

    for(auto filename :files){
        auto raw=new Capacitor(p,filename,true);
        Capacitor_Ptr cptr{raw};
        dariadb::Time lmin=dariadb::MAX_TIME, lmax=dariadb::MIN_TIME;
        if(cptr->minMaxTime(id,&lmin,&lmax)){
            res=true;
            *minResult=std::min(lmin,*minResult);
            *maxResult=std::max(lmax,*maxResult);
        }
    }
    return res;
}

Reader_ptr CapacitorManager::readInterval(const QueryInterval & q){
	return Reader_ptr();
}

Reader_ptr CapacitorManager::readInTimePoint(const QueryTimePoint & q){
	return Reader_ptr();
}

Reader_ptr CapacitorManager::currentValue(const IdArray & ids, const Flag & flag){
	TP_Reader *raw = new TP_Reader;
	auto files = cap_files();
	
	auto p = Capacitor::Params(_params.B, _params.path);
	dariadb::Meas::Id2Meas meases;
	for (const auto &f : files) {
		auto c = Capacitor_Ptr{ new Capacitor(p,f) };
		auto sub_rdr=c->currentValue(ids, flag);
		Meas::MeasList out;
		sub_rdr->readAll(&out);

		for (auto &m : out) {
			auto it = meases.find(m.id);
			if (it == meases.end()) {
				meases.insert(std::make_pair(m.id, m));
			}
			else {
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

dariadb::append_result CapacitorManager::append(const Meas & value){
    boost::upgrade_lock<boost::shared_mutex> lg(_locker);
    auto res=_cap->append(value);
    if(res.writed!=1){
        create_new();
        return _cap->append(value);
    }else{
        return res;
    }
}

void CapacitorManager::flush(){
}

void CapacitorManager::subscribe(const IdArray & ids, const Flag & flag, const ReaderClb_ptr & clbk){
	NOT_IMPLEMENTED;
}
