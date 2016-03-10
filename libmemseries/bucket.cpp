#include "bucket.h"
#include "time_ordered_set.h"
#include "utils.h"
#include <algorithm>
#include <vector>
#include <map>
#include <limits>
#include <utility>

using namespace memseries;
using namespace memseries::storage;

class Bucket::Private
{
public:
    typedef std::shared_ptr<TimeOrderedSet>   tos_ptr;
    typedef std::map<memseries::Id,tos_ptr> dict;

    Private(const size_t max_size,const size_t count, const AbstractStorage_ptr stor):
        _max_size(max_size),
        _count(count),
        _minTime(std::numeric_limits<memseries::Time>::max()),
        _maxTime(std::numeric_limits<memseries::Time>::min()),
        _bucks(),
        _stor(stor),
        _writed_count(0)
    {
    }

    Private(const Private &other) :
        _max_size(other._max_size),
        _count(other._count),
        _minTime(other._minTime),
        _maxTime(other._maxTime),
        _bucks(other._bucks),
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
            for(auto kv:_bucks){
//                if()
//                auto small=_bucks.front();
//                _bucks.erase(_bucks.begin());
//                auto arr=small->as_array();
//                auto stor_res=_stor->append(arr);
//                _writed_count -= arr.size();
//                if (stor_res.writed!=arr.size()) {
//                    return false;
//                }
            }
        }

        tos_ptr target = get_target_to_write(m);

        if (target != nullptr) {
            target->append(m, true);
            _writed_count++;
            _minTime = std::min(_minTime, m.time);
            _maxTime = std::max(_maxTime, m.time);
            return true;
        }else{
            return false;
        }
    }

    tos_ptr get_target_to_write(const Meas&m) {
        auto target=_bucks.find(m.id);
        if(target==_bucks.end()){
            auto new_target=alloc_new();
            _bucks[m.id]=new_target;
            return new_target;
        }
        return target->second;
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
            auto arr = v.second->as_array();
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
    }
protected:
    size_t _max_size;
    size_t _count;

    memseries::Time _minTime;
    memseries::Time _maxTime;

    dict _bucks;
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
