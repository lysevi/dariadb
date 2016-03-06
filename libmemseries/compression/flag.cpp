#include "flag.h"

#include <sstream>
#include<cassert>
#include <limits>

using namespace memseries::compression;

FlagCompressor::FlagCompressor(const BinaryBuffer & bw):
    _bw(bw),
    _is_first(true),
    _first(0)
{
}

FlagCompressor::~FlagCompressor()
{
}


FlagCompressor::FlagCompressor(const FlagCompressor &other):
    _bw(other._bw),
    _is_first(other._is_first),
    _first(other._first)
{}


void FlagCompressor::swap(FlagCompressor &other){
    std::swap(_is_first,other._is_first);
    std::swap(_bw,other._bw);
    std::swap(_first,other._first);
}

FlagCompressor& FlagCompressor::operator=(FlagCompressor &other){
    if(this==&other){
        return *this;
    }
    FlagCompressor tmp(other);
    this->swap(tmp);
    return *this;
}

FlagCompressor& FlagCompressor::operator=(FlagCompressor &&other){
    if(this==&other){
        return *this;
    }
    FlagCompressor tmp(std::move(other));
    this->swap(tmp);
    return *this;
}

bool FlagCompressor::append(memseries::Flag v)
{
    static_assert(sizeof(memseries::Flag)==4,"Flag no x32 value");
    if (_is_first) {
        this->_first = v;
        this->_is_first = false;
        return true;
    }

    if (v == _first) {
        if (_bw.free_size() == 1) {
            return false;
        }
        _bw.clrbit().incbit();
    }
    else {
        if (_bw.free_size() < 9) {
            return false;
        }
        _bw.setbit().incbit();
        _bw.write(uint64_t(v),31);

        _first = v;
    }
    return true;
}

FlagDeCompressor::FlagDeCompressor(const BinaryBuffer & bw, memseries::Flag first):
    _bw(bw),
    _prev_value(first){
}

FlagDeCompressor::FlagDeCompressor(const FlagDeCompressor &other):
    _bw(other._bw),
    _prev_value(other._prev_value)
{}


void FlagDeCompressor::swap(FlagDeCompressor &other){
    std::swap(_bw,other._bw);
    std::swap(_prev_value,other._prev_value);
}

FlagDeCompressor& FlagDeCompressor::operator=(FlagDeCompressor &other){
    FlagDeCompressor tmp(other);
    this->swap(tmp);
    return *this;
}

memseries::Flag FlagDeCompressor::read()
{
    static_assert(sizeof(memseries::Flag) == 4, "Flag no x32 value");
    memseries::Flag result(0);
    if (_bw.getbit() == 0) {
        _bw.incbit();
        result = _prev_value;
    }
    else {
        _bw.incbit();
        result=(memseries::Flag)_bw.read(31);
    }

    return result;
}
