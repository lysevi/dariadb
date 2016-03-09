#include "Bucket.h"
#include "time_ordered_set.h"
#include "utils.h"
#include <algorithm>
#include <list>
#include <limits>
#include <utility>

using namespace memseries;
using namespace memseries::storage;

class Bucket::Private
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


        if((maxTime()<=m.time)
                ||(utils::inInterval(_last->minTime(),_last->maxTime(),m.time))){
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

Bucket::Bucket():_Impl(new Bucket::Private(0,0))
{}

Bucket::~Bucket()
{}

Bucket::Bucket(const size_t max_size,const size_t count) :_Impl(new Bucket::Private(max_size,count))
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
