#include "membucket.h"
#include "time_ordered_set.h"
#include "utils.h"
#include <algorithm>
#include <list>
#include <limits>
#include <utility>

using namespace memseries;
using namespace memseries::storage;

class MemBucket::Private
{
public:
    typedef std::shared_ptr<TimeOrderedSet> tos_ptr;
    typedef std::list<tos_ptr>            container;

    Private(const size_t max_size,const size_t count):
        _max_size(max_size),
        _count(count),
        _minTime(std::numeric_limits<memseries::Time>::max()),
        _maxTime(std::numeric_limits<memseries::Time>::min()),
        _bucks(),
        _last(nullptr)
    {
        _bucks.push_back(alloc_new());
        _last=_bucks.front();
    }

    tos_ptr alloc_new(){
        return std::make_shared<TimeOrderedSet>(_max_size);
    }

    Private(const Private &other) :
        _max_size(other._max_size),
        _count(other._count),
        _minTime(other._minTime),
        _maxTime(other._maxTime),
        _bucks(other._bucks),
        _last(other._last)
    {}

    ~Private() {
    }

    bool append(const memseries::Meas&m) {
        bool writed=false;


        if((_last->maxTime()<=m.time)){
            if(!_last->append(m)){
                if(_bucks.size()>_count){
                    return false;
                }

                _last=alloc_new();
                _bucks.push_back(_last);
                _last->append(m);
                writed=true;
            }else{
                 writed=true;
            }
        }


        if(!writed){
            return false;
        }
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
        for(auto&tos:_bucks){
            if (!tos->is_full()){
                return false;
            }
        }
        return true;
    }
protected:
    size_t _max_size;
    size_t _count;

    memseries::Time _minTime;
    memseries::Time _maxTime;

    container _bucks;
    tos_ptr   _last;
};

MemBucket::MemBucket() :_Impl(new MemBucket::Private(0,0))
{}

MemBucket::~MemBucket()
{}

MemBucket::MemBucket(const size_t max_size,const size_t count) :_Impl(new MemBucket::Private(max_size,count))
{}

MemBucket::MemBucket(const MemBucket & other): _Impl(new MemBucket::Private(*other._Impl))
{}

MemBucket::MemBucket(MemBucket && other): _Impl(std::move(other._Impl))
{}

void MemBucket::swap(MemBucket & other)throw(){
    std::swap(_Impl, other._Impl);
}

MemBucket& MemBucket::operator=(const MemBucket & other){
    if (this != &other) {
        MemBucket tmp(other);
        this->swap(tmp);
    }
    return *this;
}

MemBucket& MemBucket::operator=(MemBucket && other){
    this->swap(other);
    return *this;
}

bool MemBucket::append(const Meas & m){
    return _Impl->append(m);
}

size_t MemBucket::size()const {
    return _Impl->size();
}

size_t MemBucket::max_size()const {
    return _Impl->max_size();
}

memseries::Time MemBucket::minTime()const {
    return _Impl->minTime();
}

memseries::Time MemBucket::maxTime()const {
    return _Impl->maxTime();
}

bool MemBucket::is_full()const {
    return _Impl->is_full();
}
