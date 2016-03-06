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

FlagCompressor::~FlagCompressor(){
}


bool FlagCompressor::append(memseries::Flag v){

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

memseries::Flag FlagDeCompressor::read(){
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
