#include "bucket.h"
#include "time_ordered_set.h"
#include "utils.h"
#include <algorithm>
#include <vector>
#include <limits>
#include <utility>

using namespace memseries;
using namespace memseries::storage;

class Bucket::Private
{
public:
    typedef std::shared_ptr<TimeOrderedSet> tos_ptr;
    typedef std::vector<tos_ptr>            container;

    Private(const size_t max_size,const size_t count, const AbstractStorage_ptr stor):
        _max_size(max_size),
        _count(count),
        _minTime(std::numeric_limits<memseries::Time>::max()),
        _maxTime(std::numeric_limits<memseries::Time>::min()),
        _bucks(),
        _last(nullptr),
		_stor(stor),
		_writed_count(0)
    {
        _bucks.push_back(alloc_new());
        _last=_bucks.front();
    }

     Private(const Private &other) :
        _max_size(other._max_size),
        _count(other._count),
        _minTime(other._minTime),
        _maxTime(other._maxTime),
        _bucks(other._bucks),
        _last(other._last),
		_stor(other._stor),
		_writed_count(other._writed_count)
    {}

    ~Private() {
    }

	tos_ptr alloc_new() {
		return std::make_shared<TimeOrderedSet>(_max_size);
	}

    bool append(const memseries::Meas&m) {
		if (is_full()) {
			auto small=_bucks.front();
			_bucks.erase(_bucks.begin());
			auto arr=small->as_array();
			auto stor_res=_stor->append(arr);
			_writed_count -= arr.size();
			if (stor_res.writed!=arr.size()) {
				return false;
			}
		}
        bool is_writed=false;

        if((maxTime()<=m.time)||(utils::inInterval(_last->minTime(),_last->maxTime(),m.time))){
            if(!_last->append(m)){
                if(_bucks.size()>_count){
                    return false;
                }

                _last=alloc_new();
                _bucks.push_back(_last);
                _last->append(m);
                is_writed=true;
            }else{
                is_writed=true;
            }
        }else{
            auto it=_bucks.begin();
            tos_ptr target=nullptr;
            for(;it!=_bucks.end();++it){
                auto b=*it;
                //insert in midle
                if(utils::inInterval(b->minTime(),b->maxTime(),m.time)){
                    target=b;
                    break;
                }else{
                    //if time between of bucks;
                    if(b->maxTime()<m.time){
                        auto new_it=it;
                        new_it++;
                        //insert in next
                        if(new_it!=_bucks.end()){
                            target=*new_it;
                        }else{//insert in cur
                            target=b;
                            break;
                        }
                    }else{//insert in cur
                        target=b;
                        break;
                    }
                }
            }
            if(target!=nullptr){
                target->append(m,true);
                is_writed=true;
            }
        }


        if(!is_writed){
            return false;
        }
		_writed_count++;
        _minTime = std::min(_minTime, m.time);
        _maxTime = std::max(_maxTime, m.time);
        return true;
    }


    size_t size()const {
        return _bucks.size();
    }

    size_t max_size()const {
        return _count;
    }

    memseries::Time minTime()const {
        return _minTime;
    }
    memseries::Time maxTime()const {
        return _maxTime;
    }

    bool is_full()const{
		return _writed_count == (_max_size*_count);
    }

	size_t writed_count()const {
		return _writed_count;
	}

	bool flush(){
		for (auto v : _bucks) {
			auto arr = v->as_array();
			auto stor_res = _stor->append(arr);
			_writed_count -= arr.size();
			if (stor_res.writed != arr.size()) {
				return false;
			}
		}
		clear();
		return true;
	}
	
	void clear(){
		this->_bucks.clear();
		_last = alloc_new();
	}
protected:
    size_t _max_size;
    size_t _count;

    memseries::Time _minTime;
    memseries::Time _maxTime;

    container _bucks;
    tos_ptr   _last;
	AbstractStorage_ptr _stor;
	size_t _writed_count;
};

Bucket::Bucket():_Impl(new Bucket::Private(0,0,nullptr))
{}

Bucket::~Bucket()
{}

Bucket::Bucket(const size_t max_size,const size_t count, const AbstractStorage_ptr stor) :_Impl(new Bucket::Private(max_size,count,stor))
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

size_t Bucket::max_size()const {
    return _Impl->max_size();
}

memseries::Time Bucket::minTime()const {
    return _Impl->minTime();
}

memseries::Time Bucket::maxTime()const {
    return _Impl->maxTime();
}

bool Bucket::is_full()const {
    return _Impl->is_full();
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