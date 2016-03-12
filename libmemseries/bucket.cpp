#include "bucket.h"
#include "time_ordered_set.h"
#include "utils.h"
#include "timeutil.h"
#include <cassert>
#include <algorithm>
#include <list>
#include <map>
#include <limits>
#include <utility>

using namespace memseries;
using namespace memseries::storage;

class Bucket::Private
{
public:
    typedef std::shared_ptr<TimeOrderedSet>   tos_ptr;
    typedef std::list<tos_ptr>                container;
	//TODO remove dict if unneeded
    typedef std::map<memseries::Id,container> dict;

	Private(const size_t max_size, const AbstractStorage_ptr stor, const memseries::Time write_window_deep):

		_max_size(max_size),
		_minTime(std::numeric_limits<memseries::Time>::max()),
		_maxTime(std::numeric_limits<memseries::Time>::min()),
		_bucks(),
		_last(nullptr),
		_stor(stor),
		_writed_count(0),
		_write_window_deep(write_window_deep)
    {
        _bucks.push_back(alloc_new());
        _last=_bucks.front();
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

    bool is_valid_time(const memseries::Time &t)const {
        auto now=memseries::timeutil::current_time();
        auto past=(now-_write_window_deep);
        if(t< past){
            return false;
        }
        return true;
    }

	bool is_valid(const memseries::Meas &m)const {
        return is_valid_time(m.time);
	}

    bool append(const memseries::Meas&m) {
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

        while(_bucks.size()>0){
            auto f=_bucks.front();
            if(is_valid_time(f->maxTime())){
                break;
            }else{
                flush_bucket(f);
                _bucks.pop_front();
            }
        }
        return true;
    }

    tos_ptr get_target_to_write(const Meas&m) {
        if ((maxTime() <= m.time) || (_last->inInterval(m))) {
            if (_last->is_full()) {
                _last = alloc_new();
                _bucks.push_back(_last);
            }
            return _last;
        }
        else {
            auto it = _bucks.rbegin();

            for (; it != _bucks.rend(); ++it) {
                auto b = *it;
                if ((b->inInterval(m))|| (b->maxTime()<m.time)) {
                    if(b->is_full()){
                        auto new_b = alloc_new();
                        _bucks.insert(it.base(),new_b);
                        return new_b;
                    }
                    return b;
                }
            }

            auto new_b = alloc_new();
            _bucks.push_front(new_b);
            return new_b;


        }
        return nullptr;
    }

    size_t size()const {
        return _bucks.size();
    }

    memseries::Time minTime()const {
        return _minTime;
    }
    memseries::Time maxTime()const {
        return _maxTime;
    }

    size_t writed_count()const {
        return _writed_count;
    }

    bool flush(){
        for (auto v : _bucks) {
            if(!flush_bucket(v)){
                return false;
            }
        }
        clear();
        return true;
    }

    bool flush_bucket(tos_ptr b){
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
        _last = alloc_new();
    }
protected:
    size_t _max_size;

    memseries::Time _minTime;
    memseries::Time _maxTime;

    container _bucks;
    tos_ptr   _last;
    AbstractStorage_ptr _stor;
    size_t _writed_count;
	memseries::Time _write_window_deep;
};

Bucket::Bucket():_Impl(new Bucket::Private(0,nullptr,0))
{}

Bucket::~Bucket()
{}

Bucket::Bucket(const size_t max_size, const AbstractStorage_ptr stor, const memseries::Time write_window_deep) :
	_Impl(new Bucket::Private(max_size,stor,write_window_deep))
{}

Bucket::Bucket(const Bucket & other): _Impl(new Bucket::Private(*other._Impl))
{}

Bucket::Bucket(Bucket && other): _Impl(std::move(other._Impl))
{}

void Bucket::swap(Bucket & other)throw(){
    std::swap(_Impl, other._Impl);
}

Bucket& Bucket::operator=(const Bucket & other){
    if (this != &other) {
        Bucket tmp(other);
        this->swap(tmp);
    }
    return *this;
}

Bucket& Bucket::operator=(Bucket && other){
    this->swap(other);
    return *this;
}

bool Bucket::append(const Meas & m){
    return _Impl->append(m);
}

size_t Bucket::size()const {
    return _Impl->size();
}

memseries::Time Bucket::minTime()const {
    return _Impl->minTime();
}

memseries::Time Bucket::maxTime()const {
    return _Impl->maxTime();
}

size_t Bucket::writed_count()const {
    return _Impl->writed_count();
}

bool Bucket::flush() {//write all to storage;
    return _Impl->flush();
}

void Bucket::clear() {
    return _Impl->clear();
}
