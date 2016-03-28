#include "capacitor.h"
#include "time_ordered_set.h"
#include "../utils/utils.h"
#include "../timeutil.h"
#include <cassert>
#include <algorithm>
#include <list>
#include <map>
#include <limits>
#include <utility>
#include <mutex>

using namespace dariadb;
using namespace dariadb::storage;

class Capacitor::Private
{
public:
    typedef std::shared_ptr<TimeOrderedSet>   tos_ptr;
    typedef std::list<tos_ptr>                container;
    typedef std::map<dariadb::Id,container>   dict;
    typedef std::map<dariadb::Id, tos_ptr>    dict_last;

	Private(const size_t max_size, const AbstractStorage_ptr stor, const dariadb::Time write_window_deep):

		_max_size(max_size),
		_minTime(std::numeric_limits<dariadb::Time>::max()),
		_maxTime(std::numeric_limits<dariadb::Time>::min()),
		_bucks(),
        _last(),
		_stor(stor),
		_writed_count(0),
		_write_window_deep(write_window_deep)
    {

    }

	Private(const Private &other) :
		_max_size(other._max_size),
		_minTime(other._minTime),
		_maxTime(other._maxTime),
		_bucks(other._bucks),
		_last(other._last),
		_stor(other._stor),
		_writed_count(other._writed_count),
		_write_window_deep(other._write_window_deep)
    {}

    ~Private() {
    }

    tos_ptr alloc_new() {
        return std::make_shared<TimeOrderedSet>(_max_size);
    }

    bool is_valid_time(const dariadb::Time &t)const {
        auto now=dariadb::timeutil::current_time();
        auto past=(now-_write_window_deep);
        if(t< past){
            return false;
        }
        return true;
    }

	bool is_valid(const dariadb::Meas &m)const {
        return is_valid_time(m.time);
	}
	bool check_and_append(const dariadb::Meas&m) {
		if (!is_valid(m)) {
			return false;
		}

		tos_ptr target = get_target_to_write(m);

		if (target != nullptr) {
			target->append(m, true);
			_writed_count++;
			_minTime = std::min(_minTime, m.time);
			_maxTime = std::max(_maxTime, m.time);
		}
		return true;
	}

    bool append(const dariadb::Meas&m) {
		std::lock_guard<std::mutex> lg(_mutex);

		auto res = check_and_append(m);

        //flush old sets.
        for(auto &kv:_bucks){
            while(kv.second.size()>0){
                auto v=kv.second.front();
                if(is_valid_time(v->maxTime())){
                    break;
                }else{
                    flush_set(v);
                    kv.second.pop_front();
                }
            }
        }


        return res;
    }

    tos_ptr get_target_to_write(const Meas&m) {
//		std::lock_guard<std::mutex> lg(_mutex);
        auto last_it=_last.find(m.id);
        if(last_it ==_last.end()){
			
            auto n=alloc_new();
            _last[m.id]=n;
            _bucks[m.id].push_back(n);
            return n;
        }

        if ((maxTime() <= m.time) || (last_it->second->inInterval(m))) {
            if (last_it->second->is_full()) {
                auto n=alloc_new();
                _last[m.id]=n;
                _bucks[m.id].push_back(n);
            }
            return _last[m.id];
        }
        else {
            auto it = _bucks[m.id].rbegin();

            for (; it != _bucks[m.id].rend(); ++it) {
                auto b = *it;
                if ((b->inInterval(m))|| (b->maxTime()<m.time)) {
                    if(b->is_full()){
                        auto new_b = alloc_new();
                        _bucks[m.id].insert(it.base(),new_b);
                        return new_b;
                    }
                    return b;
                }
            }

            auto new_b = alloc_new();
            _bucks[m.id].push_front(new_b);
            return new_b;


        }
    }

    size_t size()const {
        return _bucks.size();
    }

    dariadb::Time minTime()const {
        return _minTime;
    }
    dariadb::Time maxTime()const {
        return _maxTime;
    }

    size_t writed_count()const {
        return _writed_count;
    }

    bool flush(){
		std::lock_guard<std::mutex> lg(_mutex);
        for (auto kv : _bucks) {
            for(auto v:kv.second){
                if(!flush_set(v)){
                    return false;
                }
            }
        }
        clear();
        return true;
    }

    bool flush_set(tos_ptr b){
        auto arr = b->as_array();
        auto stor_res = _stor->append(arr);
        _writed_count -= arr.size();
        if (stor_res.writed != arr.size()) {
            return false;
        }
        return true;
    }

    void clear(){
        this->_bucks.clear();
        _last.clear();
    }
protected:
    size_t _max_size;

    dariadb::Time _minTime;
    dariadb::Time _maxTime;

    dict _bucks;
    dict_last   _last;
    AbstractStorage_ptr _stor;
    size_t _writed_count;
	dariadb::Time _write_window_deep;
	std::mutex _mutex;
};

Capacitor::Capacitor():_Impl(new Capacitor::Private(0,nullptr,0))
{}

Capacitor::~Capacitor()
{}

Capacitor::Capacitor(const size_t max_size, const AbstractStorage_ptr stor, const dariadb::Time write_window_deep) :
	_Impl(new Capacitor::Private(max_size,stor,write_window_deep))
{}

Capacitor::Capacitor(const Capacitor & other): _Impl(new Capacitor::Private(*other._Impl))
{}

Capacitor::Capacitor(Capacitor && other): _Impl(std::move(other._Impl))
{}

void Capacitor::swap(Capacitor & other)throw(){
    std::swap(_Impl, other._Impl);
}

Capacitor& Capacitor::operator=(const Capacitor & other){
    if (this != &other) {
        Capacitor tmp(other);
        this->swap(tmp);
    }
    return *this;
}

Capacitor& Capacitor::operator=(Capacitor && other){
    this->swap(other);
    return *this;
}

bool Capacitor::append(const Meas & m){
    return _Impl->append(m);
}

size_t Capacitor::size()const {
    return _Impl->size();
}

dariadb::Time Capacitor::minTime()const {
    return _Impl->minTime();
}

dariadb::Time Capacitor::maxTime()const {
    return _Impl->maxTime();
}

size_t Capacitor::writed_count()const {
    return _Impl->writed_count();
}

bool Capacitor::flush() {//write all to storage;
    return _Impl->flush();
}

void Capacitor::clear() {
    return _Impl->clear();
}