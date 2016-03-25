#include "flag.h"

#include <sstream>
#include<cassert>
#include <limits>

using namespace dariadb::compression;

FlagCompressor::FlagCompressor(const BinaryBuffer_Ptr & bw):
    BaseCompressor(bw),
    _is_first(true),
    _first(0)
{
}

FlagCompressor::~FlagCompressor(){
}


bool FlagCompressor::append(dariadb::Flag v){

    static_assert(sizeof(dariadb::Flag)==4,"Flag no x32 value");
    if (_is_first) {
        this->_first = v;
        this->_is_first = false;
        return true;
    }

    if (v == _first) {
        if (_bw->free_size() == 1) {
            return false;
        }
        _bw->clrbit().incbit();
    }
    else {
        if (_bw->free_size() < 9) {
            return false;
        }
        _bw->setbit().incbit();
        _bw->write(uint64_t(v),31);

        _first = v;
    }
    return true;
}

FlagCompressionPosition FlagCompressor::get_position()const{
    FlagCompressionPosition result;
    result._first=_first;
    result._is_first=_is_first;
    return result;
}

void FlagCompressor::restore_position(const FlagCompressionPosition&pos){
    _first=pos._first;
    _is_first=pos._is_first;
}

FlagDeCompressor::FlagDeCompressor(const BinaryBuffer_Ptr & bw, dariadb::Flag first):
	BaseCompressor(bw),
    _prev_value(first){
}

dariadb::Flag FlagDeCompressor::read(){
    static_assert(sizeof(dariadb::Flag) == 4, "Flag no x32 value");
    dariadb::Flag result(0);
    if (_bw->getbit() == 0) {
        _bw->incbit();
        result = _prev_value;
    }
    else {
        _bw->incbit();
        result=(dariadb::Flag)_bw->read(31);
    }

    return result;
}
