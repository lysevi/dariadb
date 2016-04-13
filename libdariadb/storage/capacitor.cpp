#include "capacitor.h"
#include "time_ordered_set.h"
#include "../utils/utils.h"
#include "../utils/locker.h"
#include "../timeutil.h"
#include <cassert>
#include <algorithm>
#include <list>
#include <map>
#include <limits>
#include <utility>

using namespace dariadb;
using namespace dariadb::storage;

class Capacitor::Private
{
public:
    typedef std::shared_ptr<TimeOrderedSet>   tos_ptr;
    typedef std::list<tos_ptr>                container;
    typedef std::map<dariadb::Id,container>   dict;
    typedef std::map<dariadb::Id, tos_ptr>    dict_last;

	Private(const BaseStorage_ptr stor, const  Capacitor::Params&params):
		_minTime(std::numeric_limits<dariadb::Time>::max()),
		_maxTime(std::numeric_limits<dariadb::Time>::min()),
		_bucks(),
        _last(),
		_stor(stor),
		_writed_count(0),
		_params(params)
    {

    }

	Private(const Private &other) :
		_minTime(other._minTime),
		_maxTime(other._maxTime),
		_bucks(other._bucks),
		_last(other._last),
		_stor(other._stor),
		_writed_count(other._writed_count),
		_params(other._params)
    {}

    ~Private() {
    }

    tos_ptr alloc_new() {
        return std::make_shared<TimeOrderedSet>(_params.max_size);
    }

    bool is_valid_time(const dariadb::Time &t)const {
        auto now=dariadb::timeutil::current_time();
		auto past = (now - _params.write_window_deep);
		if (t< past) {
			return false;
		}
		else {
			return true;
		}
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
            return true;
        }else{
            assert(false);
            return false;
        }

	}

    bool append(const dariadb::Meas&m) {
        std::lock_guard<dariadb::utils::Locker> lg(_locker);

		auto res = check_and_append(m);
        return res;
    }

	void flush_old_sets() {
        std::lock_guard<dariadb::utils::Locker> lg(_locker);
		for (auto &kv : _bucks) {
			bool flushed = false;
			//while (kv.second.size()>0) 
			for (auto it = kv.second.begin(); it != kv.second.end();++it)
			{
				auto v = *it;
				if (!is_valid_time(v->maxTime())) {
					flush_set(v);
					flushed = true;
					v->is_dropped = true;
				}
			}
			if (flushed) {
				auto r_if_it = std::remove_if(
					kv.second.begin(),
					kv.second.end(),
					[this](const tos_ptr c)
				{
					return c->is_dropped;
				});

				kv.second.erase(r_if_it, kv.second.end());
			}
		}
	}

    tos_ptr get_target_to_write(const Meas&m) {
//		std::lock_guard<dariadb::utils::Locker> lg(_locker);
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
        std::lock_guard<dariadb::utils::Locker> lg(_locker);

		for (auto &kv : _bucks) {
            for(auto &v:kv.second){
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

	dariadb::Time get_write_window_deep()const{
		return _params.write_window_deep;
	}
protected:
    dariadb::Time _minTime;
    dariadb::Time _maxTime;

    dict _bucks;
    dict_last   _last;
    BaseStorage_ptr _stor;
    size_t _writed_count;
    dariadb::utils::Locker _locker;
	Capacitor::Params _params;
};

Capacitor::~Capacitor(){
    this->stop_worker();
}

Capacitor::Capacitor(const BaseStorage_ptr stor, const Params&params) :
	dariadb::utils::PeriodWorker(std::chrono::milliseconds(params.write_window_deep + capasitor_sync_delta)),
	_Impl(new Capacitor::Private(stor,params))
{
    this->start_worker();
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

void Capacitor::call(){
	this->_Impl->flush_old_sets();
}
