#include "aof_manager.h"
#include "manifest.h"
#include "../utils/exception.h"
#include "../utils/fs.h"
#include "../flags.h"
#include "inner_readers.h"
#include <cassert>
#include <iterator>

using namespace dariadb::storage;
using namespace dariadb;

AOFManager *AOFManager::_instance = nullptr;

AOFManager::~AOFManager(){
	this->flush();
}

AOFManager::AOFManager(const Params & param):_params(param){
	_down = nullptr;
    if (!dariadb::utils::fs::path_exists(_params.path)) {
      dariadb::utils::fs::mkdir(_params.path);
    }
	_buffer.resize(_params.buffer_size);
	_buffer_pos = 0;
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
    _aof=nullptr;
     auto p=AOFile::Params(_params.max_size, _params.path);
     /*if(_down!=nullptr){
         auto closed=this->closed_caps();
		 const size_t MAX_CLOSED_CAPS = 10;
         if(closed.size()>MAX_CLOSED_CAPS){
             size_t to_drop=closed.size()/2;
             for(size_t i=0;i<to_drop;++i){
                 auto f=closed.front();
                 closed.pop_front();
                 this->drop_cap(f,_down);
             }
         }
     }*/
    _aof=AOFile_Ptr{new AOFile(p)};
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

std::list<std::string> dariadb::storage::AOFManager::closed_aofs(){
    auto all_files=aof_files();
    std::list<std::string> result;
    for(auto fn:all_files){
        if(_aof==nullptr){
            result.push_back(fn);
        }else{
            if(fn!=this->_aof->filename()){
                result.push_back(fn);
            }
        }
    }
    return result;
}

void dariadb::storage::AOFManager::drop_aof(const std::string & fname, MeasWriter* storage){
    auto p=AOFile::Params(_params.max_size,_params.path);
    AOFile aof{p,fname, false};
    aof.drop_to_stor(storage);
    utils::fs::rm(fname);
    auto without_path = utils::fs::extract_filename(fname);
    Manifest::instance()->aof_rm(without_path);
}


dariadb::Time AOFManager::minTime(){
    std::lock_guard<std::mutex> lg(_locker);
    auto files = aof_files();
	dariadb::Time result = dariadb::MAX_TIME;
    for (auto filename : files) {
        auto p=AOFile::Params(_params.max_size,_params.path);
        AOFile aof(p,filename,true);
        auto local =aof.minTime();
        result = std::min(local, result);
    }
    size_t pos=0;
    for(auto v:_buffer){
       result = std::min(v.time, result);
       ++pos;
       if(pos>_buffer_pos){
           break;
       }
    }
	return result;
}

dariadb::Time AOFManager::maxTime(){
    std::lock_guard<std::mutex> lg(_locker);
    auto files = aof_files();
    dariadb::Time result = dariadb::MIN_TIME;
    for (auto filename : files) {
        auto p=AOFile::Params(_params.max_size,_params.path);
        AOFile aof(p,filename,true);
        auto local =aof.maxTime();
        result = std::max(local, result);
    }
    for(auto v:_buffer){
       result = std::max(v.time, result);
    }
	return result;
}

bool AOFManager::minMaxTime(dariadb::Id id, dariadb::Time * minResult, dariadb::Time * maxResult){
    std::lock_guard<std::mutex> lg(_locker);
    auto files = aof_files();
    auto p=AOFile::Params(_params.max_size,_params.path);

    bool res=false;
    *minResult=dariadb::MAX_TIME;
    *maxResult=dariadb::MIN_TIME;

    for(auto filename :files){
        AOFile aof(p,filename,true);
        dariadb::Time lmin=dariadb::MAX_TIME, lmax=dariadb::MIN_TIME;
        if(aof.minMaxTime(id,&lmin,&lmax)){
            res=true;
            *minResult=std::min(lmin,*minResult);
            *maxResult=std::max(lmax,*maxResult);
        }
    }

    size_t pos=0;
    for(auto v:_buffer){
        if(v.id==id){
            res=true;
            *minResult=std::min(v.time,*minResult);
            *maxResult=std::max(v.time,*maxResult);
        }
        ++pos;
        if(pos>_buffer_pos){
            break;
        }
    }
    return res;
}


Reader_ptr AOFManager::readInterval(const QueryInterval & query){
    std::lock_guard<std::mutex> lg(_locker);

    auto files = aof_files();
    auto p = AOFile::Params(_params.max_size, _params.path);
    TP_Reader *raw = new TP_Reader;
    std::map<dariadb::Id, std::set<Meas, meas_time_compare_less>> sub_result;

    for (auto filename : files) {
        AOFile aof(p, filename, true);
        Meas::MeasList out;
        aof.readInterval(query)->readAll(&out);
        for (auto m : out) {
            //TODO check!
            if (m.flag == Flags::_NO_DATA) {
                continue;
            }
            sub_result[m.id].insert(m);
        }
    }
    size_t pos=0;
    for(auto v:_buffer){
       if(v.inQuery(query.ids,query.flag,query.from,query.to)){
           sub_result[v.id].insert(v);
       }
       ++pos;
       if(pos>_buffer_pos){
           break;
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

Reader_ptr AOFManager::readInTimePoint(const QueryTimePoint & query){
    std::lock_guard<std::mutex> lg(_locker);

    auto files = aof_files();
    auto p = AOFile::Params(_params.max_size, _params.path);
    TP_Reader *raw = new TP_Reader;
    dariadb::Meas::Id2Meas sub_result;

    for (auto filename : files) {
        AOFile aof(p, filename, true);
        Meas::MeasList out;
        aof.readInTimePoint(query)->readAll(&out);

        for (auto &m : out) {
            auto it = sub_result.find(m.id);
            if (it == sub_result.end()) {
                sub_result.insert(std::make_pair(m.id, m));
            }
            else {
                if (it->second.flag == Flags::_NO_DATA) {
                    sub_result[m.id] = m;
                }
            }
        }
    }

    size_t pos=0;
    for(auto v:_buffer){
        if(v.inQuery(query.ids,query.flag)){
            auto it = sub_result.find(v.id);
            if (it == sub_result.end()) {
                sub_result.insert(std::make_pair(v.id, v));
            }
            else {
                if((v.time>it->second.time) && (v.time<=query.time_point)){
                    sub_result[v.id] = v;
                }
            }
        }
        ++pos;
        if(pos>_buffer_pos){
            break;
        }
    }

    for (auto &kv : sub_result) {
        raw->_ids.push_back(kv.first);
        raw->_values.push_back(kv.second);
    }
    raw->reset();
    return Reader_ptr(raw);
}

Reader_ptr AOFManager::currentValue(const IdArray & ids, const Flag & flag){
    TP_Reader *raw = new TP_Reader;
    auto files = aof_files();
	
    auto p = AOFile::Params(_params.max_size, _params.path);
    dariadb::Meas::Id2Meas meases;
    for (const auto &f : files) {
        AOFile c(p,f, true);
        auto sub_rdr=c.currentValue(ids, flag);
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

dariadb::append_result AOFManager::append(const Meas & value){
    std::lock_guard<std::mutex> lg(_locker);
	_buffer[_buffer_pos] = value;
	_buffer_pos++;
	
	if (_buffer_pos >= _params.buffer_size) {
		Meas::MeasList ml{ _buffer.begin(),_buffer.end() };
		if (_aof == nullptr) {
			create_new();
		}
		while (1) {
			auto res = _aof->append(ml);
			if (res.writed != ml.size()) {
				create_new();
				auto it = ml.begin();
				std::advance(it, res.writed);
				ml.erase(ml.begin(), it);
			}
			else {
				break;
			}
		}
		_buffer_pos = 0;
	}
    return dariadb::append_result(1,0);

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
