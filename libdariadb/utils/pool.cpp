#include "pool.h"
#include <cassert>
#include <cstring>
#include <iostream>

using namespace dariadb::utils;

Pool::Pool(size_t max_size):_max_size(max_size){
}

Pool::~Pool(){
    while (!_ptrs.empty()) {
        void *out=_ptrs.front();
        _ptrs.pop();
        if(out!=nullptr){
            :: operator delete(out);
        }
    }
}


size_t Pool::polled(){
    _locker.lock();
    auto result=_ptrs.size();
    _locker.unlock();
    return result;
}

void* Pool::alloc(std::size_t sz) {
    _locker.lock();
    void*result = nullptr;
    if(!_ptrs.empty()){
        result=_ptrs.front();
        _ptrs.pop();
        _locker.unlock();
    }else {
        _locker.unlock();
        result = ::operator new(sz);
    }

    memset(result, 0, sz);
    return result;
}

void Pool::free(void* ptr, std::size_t){
    _locker.lock();
    if (_ptrs.size()<_max_size) {
        _ptrs.push(ptr);
        _locker.unlock();
    }
    else {
        _locker.unlock();
        ::operator delete(ptr);
    }
}
