#include "compression.h"
#include "utils.h"
#include <exception>

using namespace timedb::compression;

const uint16_t delta_64_mask = 512;         //10 0000 0000
const uint16_t delta_256_mask = 3072;       //1100 0000 0000
const uint16_t delta_2047_mask = 57344;     //1110 0000 0000 0000
const uint64_t delta_big_mask = 64424509440;//1111 [0000 0000] [0000 0000][0000 0000] [0000 0000]

DeltaCompressor::DeltaCompressor(const BinaryWriter &bw):
    _is_first(true),
    _bw(bw),
    _first(0),
    _prev_delta(0),
    _prev_time(0)
{
}


DeltaCompressor::~DeltaCompressor(){
}

void DeltaCompressor::append(timedb::Time t){
    if(_is_first){
        _first=t;
        _is_first=false;
        _prev_time=t;
        return;
    }

    int64_t D=(t-_prev_time) - _prev_delta;
    if(D==0){
        _bw.clrbit().incbit();
    }else{
        if ((-63<D)&&(D<64)){
            auto d=DeltaCompressor::get_delta_64(D);
            for(auto i=9;i>=0;i--){
                if(utils::BitOperations::check(d,i)){
                    _bw.setbit().incbit();
                }else{
                    _bw.clrbit().incbit();
                }
            }
        }else{
            if ((-255<D)&&(D<256)){
                auto d=DeltaCompressor::get_delta_256(D);
                for(auto i=11;i>=0;i--){
                    if(utils::BitOperations::check(d,i)){
                        _bw.setbit().incbit();
                    }else{
                        _bw.clrbit().incbit();
                    }
                }
            }else{
                if ((-2047<D)&&(D<2048)){
                    auto d=DeltaCompressor::get_delta_2048(D);
                    for(auto i=15;i>=0;i--){
                        if(utils::BitOperations::check(d,i)){
                            _bw.setbit().incbit();
                        }else{
                            _bw.clrbit().incbit();
                        }
                    }
                }else{
                    auto d=DeltaCompressor::get_delta_big(D);
                    for(auto i=35;i>=0;i--){
                        if(utils::BitOperations::check(d,i)){
                            _bw.setbit().incbit();
                        }else{
                            _bw.clrbit().incbit();
                        }
                    }
                }
            }
        }
    }

    _prev_delta=D;
    _prev_time=t;
}


uint16_t DeltaCompressor::get_delta_64(int64_t D) {
    return delta_64_mask | static_cast<uint16_t>(D);
}

uint16_t DeltaCompressor::get_delta_256(int64_t D) {
	return delta_256_mask| static_cast<uint16_t>(D);
}

uint16_t DeltaCompressor::get_delta_2048(int64_t D) {
	return delta_2047_mask | static_cast<uint16_t>(D);
}


uint64_t DeltaCompressor::get_delta_big(int64_t D) {
	return delta_big_mask | D;
}

DeltaDeCompressor::DeltaDeCompressor(const BinaryWriter &bw, Time first):
    _bw(bw),
    _prev_delta(0),
    _prev_time(first)
{

}

DeltaDeCompressor::~DeltaDeCompressor(){

}

timedb::Time DeltaDeCompressor::read(){
    auto b0=_bw.getbit();
    _bw.incbit();

    if(b0==0){
        return _prev_time+_prev_delta;
    }

    auto b1=_bw.getbit();
    _bw.incbit();
    if((b0==1) && (b1==0)){//64
        timedb::Time result(0);
        for(int i=7;i>=0;i--){
            if(_bw.getbit()==1){
                result=utils::BitOperations::set(result,i);
            }else{
                result=utils::BitOperations::clr(result,i);
            }
            _bw.incbit();
        }

        auto ret=_prev_time+result+_prev_delta;
        _prev_delta=result;
        _prev_time=ret;
        return ret;
    }

    auto b2=_bw.getbit();
    _bw.incbit();
    if((b0==1) && (b1==1)&& (b2==0)){//256
        timedb::Time result(0);
        for(int i=8;i>=0;i--){
            if(_bw.getbit()==1){
                result=utils::BitOperations::set(result,i);
            }else{
                result=utils::BitOperations::clr(result,i);
            }
            _bw.incbit();
        }

        auto ret=_prev_time+result+_prev_delta;
        _prev_delta=result;
        _prev_time=ret;
        return ret;
    }

    auto b3=_bw.getbit();
    _bw.incbit();
    if((b0==1) && (b1==1)&& (b2==1)&& (b3==0)){//2048
        timedb::Time result(0);
        for(int i=11;i>=0;i--){
            if(_bw.getbit()==1){
                result=utils::BitOperations::set(result,i);
            }else{
                result=utils::BitOperations::clr(result,i);
            }
            _bw.incbit();
        }

        auto ret=_prev_time+result+_prev_delta;
        _prev_delta=result;
        _prev_time=ret;
        return ret;
    }

    int64_t result(0);
    for(int i=31;i>=0;i--){
        if(_bw.getbit()==1){
            result=utils::BitOperations::set(result,i);
        }else{
            result=utils::BitOperations::clr(result,i);
        }
        _bw.incbit();
    }

    auto ret=_prev_time+result+_prev_delta;
    _prev_delta=result;
    _prev_time=ret;
    return ret;
}

BinaryWriter::BinaryWriter(uint8_t* begin,uint8_t*end):
    _begin(begin),
    _end(end),
    _cap(std::distance(begin,end)),
    _pos(0),
    _bitnum(max_bit_pos){
}

BinaryWriter::BinaryWriter(const BinaryWriter & other) {
    this->_begin = other._begin;
    this->_end = other._end;
	_pos = other._pos;
	_bitnum = other._bitnum;
	_cap = other._cap;
}

BinaryWriter::BinaryWriter(BinaryWriter && other){
    _begin = other._begin;
    _end = other._end;
	_pos = other._pos;
	_cap = other._cap;
	_bitnum = other._bitnum;
	other._pos = 0;
	other._cap = 0;
    other._bitnum = 0;
    other._begin=other._end=nullptr;
}

BinaryWriter::~BinaryWriter(){
}

BinaryWriter& BinaryWriter::operator=(const BinaryWriter & other){
    if(this!=&other){
        BinaryWriter tmp(other);
        tmp.swap(*this);
    }
	return *this;
}

void BinaryWriter::swap(BinaryWriter & other) throw(){
	std::swap(_pos, other._pos);
	std::swap(_cap, other._cap);
    std::swap(_begin, other._begin);
    std::swap(_end, other._end);
	std::swap(_bitnum, other._bitnum);
}

BinaryWriter& BinaryWriter::incbit(){
	_bitnum--;
	if (_bitnum < 0) {
		incpos();
		_bitnum = max_bit_pos;
	}
    return *this;
}

BinaryWriter& BinaryWriter::incpos(){
	_pos++;
    if (_pos>=_cap){
        throw std::logic_error("BinaryWriter::incpos");
    }
    return *this;
}

void BinaryWriter::set_bitnum(size_t num) {
	this->_bitnum = num;
}

void BinaryWriter::set_pos(size_t pos) {
	this->_pos = pos;
}

void BinaryWriter::reset_pos() {
	this->set_pos(0); 
	this->set_bitnum(max_bit_pos);
}

uint8_t BinaryWriter::getbit() const{
    return utils::BitOperations::get(_begin[_pos],_bitnum);
}


BinaryWriter& BinaryWriter::setbit() {
    _begin[_pos]=utils::BitOperations::set(_begin[_pos],_bitnum);
    return *this;
}

BinaryWriter& BinaryWriter::clrbit() {
    _begin[_pos] =utils::BitOperations::clr(_begin[_pos],_bitnum);
    return *this;
}

XorCompressor::XorCompressor(const BinaryWriter &bw):
    _is_first(true),
    _bw(bw),
    _first(0),
    _prev_value(0)
{

}

XorCompressor::~XorCompressor(){

}

void XorCompressor::append(timedb::Value v){
    if(_is_first){
        _first=v;
        _is_first=false;
        _prev_value=v;
        return;
    }
    NOT_IMPLEMENTED;
}

uint8_t XorCompressor::zeros_lead(timedb::Value v){
NOT_IMPLEMENTED;
}

uint8_t XorCompressor::zeros_tail(timedb::Value v){
NOT_IMPLEMENTED;
}
