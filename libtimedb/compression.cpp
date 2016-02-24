#include "compression.h"
#include "utils.h"
#include <exception>

using namespace timedb::compression;

const uint16_t delta_64_mask = 512;         //10 0000 0000
const uint16_t delta_64_mask_inv = 127;     //00 1111 111
const uint16_t delta_256_mask = 3072;       //1100 0000 0000
const uint16_t delta_256_mask_inv = 511;    //0001 1111 1111
const uint16_t delta_2047_mask = 57344;     //1110 0000 0000 0000
const uint16_t delta_2047_mask_inv = 4095;  //0000 1111 1111 1111
const uint64_t delta_big_mask = 64424509440;   //1111 [0000 0000] [0000 0000][0000 0000] [0000 0000]
const uint64_t delta_big_mask_inv = 4294967295;//0000 1111 1111 1111 1111 1111 1111   1111 1111

DeltaCompressor::DeltaCompressor(const BinaryBuffer &bw):
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
            //TODO move for to BinaryBuffer.
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
    return delta_64_mask |  (delta_64_mask_inv & static_cast<uint16_t>(D));
}

uint16_t DeltaCompressor::get_delta_256(int64_t D) {
	return delta_256_mask| (delta_256_mask_inv &static_cast<uint16_t>(D));
}

uint16_t DeltaCompressor::get_delta_2048(int64_t D) {
	return delta_2047_mask | (delta_2047_mask_inv &static_cast<uint16_t>(D));
}


uint64_t DeltaCompressor::get_delta_big(int64_t D) {
	return delta_big_mask | (delta_big_mask_inv & D);
}

DeltaDeCompressor::DeltaDeCompressor(const BinaryBuffer &bw, timedb::Time first):
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
        int8_t result(0);
        //TODO move to BinaryBuffer.
        for(int i=7;i>=0;i--){
            if(_bw.getbit()==1){
                result=utils::BitOperations::set(result,i);
            }else{
                result=utils::BitOperations::clr(result,i);
            }
            _bw.incbit();
        }
		if (result>64) { //is negative
			result = (-128) | result;
		}
		
        auto ret=_prev_time+result+_prev_delta;
        _prev_delta=result;
        _prev_time=ret;
        return ret;
    }

    auto b2=_bw.getbit();
    _bw.incbit();
    if((b0==1) && (b1==1)&& (b2==0)){//256
        int16_t result(0);
        for(int i=8;i>=0;i--){
            if(_bw.getbit()==1){
                result=utils::BitOperations::set(result,i);
            }else{
                result=utils::BitOperations::clr(result,i);
            }
            _bw.incbit();
        }
		if (result > 256) { //is negative
			result = (-256) | result;
		}
        auto ret=_prev_time+result+_prev_delta;
        _prev_delta=result;
        _prev_time=ret;
        return ret;
    }

    auto b3=_bw.getbit();
    _bw.incbit();
    if((b0==1) && (b1==1)&& (b2==1)&& (b3==0)){//2048
        int16_t result(0);
        for(int i=11;i>=0;i--){
            if(_bw.getbit()==1){
                result=utils::BitOperations::set(result,i);
            }else{
                result=utils::BitOperations::clr(result,i);
            }
            _bw.incbit();
        }
		if (result > 2048) { //is negative
			result = (-2048) | result;
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
	if (result > std::numeric_limits<int32_t>::max()) {
		result = (-4294967296) | result;
	}
    auto ret=_prev_time+result+_prev_delta;
    _prev_delta=result;
    _prev_time=ret;
    return ret;
}

BinaryBuffer::BinaryBuffer(uint8_t* begin,uint8_t*end):
    _begin(begin),
    _end(end),
    _cap(std::distance(begin,end)),
    _pos(0),
    _bitnum(max_bit_pos){
}

BinaryBuffer::BinaryBuffer(const BinaryBuffer & other) {
    this->_begin = other._begin;
    this->_end = other._end;
	_pos = other._pos;
	_bitnum = other._bitnum;
	_cap = other._cap;
}

BinaryBuffer::BinaryBuffer(BinaryBuffer && other){
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

BinaryBuffer::~BinaryBuffer(){
}

BinaryBuffer& BinaryBuffer::operator=(const BinaryBuffer & other){
    if(this!=&other){
        BinaryBuffer tmp(other);
        tmp.swap(*this);
    }
	return *this;
}

void BinaryBuffer::swap(BinaryBuffer & other) throw(){
	std::swap(_pos, other._pos);
	std::swap(_cap, other._cap);
    std::swap(_begin, other._begin);
    std::swap(_end, other._end);
	std::swap(_bitnum, other._bitnum);
}

BinaryBuffer& BinaryBuffer::incbit(){
	_bitnum--;
	if (_bitnum < 0) {
		incpos();
		_bitnum = max_bit_pos;
	}
    return *this;
}

BinaryBuffer& BinaryBuffer::incpos(){
	_pos++;
    if (_pos>=_cap){
        throw std::logic_error("BinaryWriter::incpos");
    }
    return *this;
}

void BinaryBuffer::set_bitnum(size_t num) {
	this->_bitnum = num;
}

void BinaryBuffer::set_pos(size_t pos) {
	this->_pos = pos;
}

void BinaryBuffer::reset_pos() {
	this->set_pos(0); 
	this->set_bitnum(max_bit_pos);
}

uint8_t BinaryBuffer::getbit() const{
    return utils::BitOperations::get(_begin[_pos],_bitnum);
}


BinaryBuffer& BinaryBuffer::setbit() {
    _begin[_pos]=utils::BitOperations::set(_begin[_pos],_bitnum);
    return *this;
}

BinaryBuffer& BinaryBuffer::clrbit() {
    _begin[_pos] =utils::BitOperations::clr(_begin[_pos],_bitnum);
    return *this;
}


std::ostream&  timedb::compression::operator<< (std::ostream& stream, const BinaryBuffer& b) {
	stream << " pos:" << b.pos() << " cap:" << b.cap() << " bit:" << b.bitnum() << " [";
	for (size_t i = 0; i <= b.pos(); i++) {
		for (int bit = int(max_bit_pos); bit >= 0; bit--) {
			auto is_cur = ((bit == b.bitnum()) && (i == b.pos()));
			if (is_cur)
				stream << "[";
			stream << ((b._begin[i] >> bit) & 1);
			if (is_cur)
				stream << "]";
			if (bit == 4) stream << " ";
		}
		stream << std::endl;
	}
	return stream << "]";
}

XorCompressor::XorCompressor(const BinaryBuffer &bw):
    _is_first(true),
    _bw(bw),
    _first(0),
    _prev_value(0)
{

}

XorCompressor::~XorCompressor(){

}

void XorCompressor::append(timedb::Value v){
	static_assert(sizeof(timedb::Value) == 8, "Value no x64 value");

    if(_is_first){
        _first=v;
        _is_first=false;
        _prev_value=v;
        return;
    }

    auto xor_val=_prev_value^v;
    if (xor_val==0){
        _bw.clrbit().incbit();
        return;
    }
    _bw.setbit().incbit();

    auto lead=zeros_lead(xor_val);
    auto tail=zeros_tail(xor_val);

    if ((_prev_lead==lead) && (_prev_tail==tail)){
        _bw.clrbit().incbit();
    }else{
        _bw.setbit().incbit();


        for (int8_t i = 5; i >= 0; i--) {
            auto b= utils::BitOperations::get(lead, i);
            if (b){
                _bw.setbit().incbit();
            }else{
                _bw.clrbit().incbit();
            }
        }

        for (int8_t i = 5; i >= 0; i--) {
            auto b= utils::BitOperations::get(tail, i);
            if (b){
                _bw.setbit().incbit();
            }else{
                _bw.clrbit().incbit();
            }
        }
    }

    for (int i = (63 - lead); i >= tail; i--) {
        auto b = utils::BitOperations::get(xor_val, i);
        if (b){
            _bw.setbit().incbit();
        }else{
            _bw.clrbit().incbit();
        }
    }

    _prev_value = v;
    _prev_lead=lead;
    _prev_tail=tail;

}

uint8_t XorCompressor::zeros_lead(timedb::Value v){
    const int max_bit_pos=sizeof(Value)*8-1;
    uint8_t result=0;
    for(int i=max_bit_pos;i>=0;i--){
            if(utils::BitOperations::check(v,i)){
                break;
            }else{
                result++;
            }
    }
    return result;
}

uint8_t XorCompressor::zeros_tail(timedb::Value v){
    const int max_bit_pos=sizeof(Value)*8-1;
    uint8_t result=0;
    for(int i=0;i<max_bit_pos;i++){
            if(utils::BitOperations::check(v,i)){
                break;
            }else{
                result++;
            }
    }
    return result;
}

XorDeCompressor::XorDeCompressor(const BinaryBuffer &bw, timedb::Value first):
    _bw(bw),
    _prev_value(first),
    _prev_lead(0),
    _prev_tail(0)
{

}

timedb::Value XorDeCompressor::read()
{
	static_assert(sizeof(timedb::Value) == 8, "Value no x64 value");
    auto b0=_bw.getbit();
    _bw.incbit();
    if(b0==0){
        return _prev_value;
    }

    auto b1=_bw.getbit();
    _bw.incbit();

    if(b1==1){
        uint8_t leading = 0;
        for (int i = 5; i >= 0; i--) {
            auto b=_bw.getbit();
            _bw.incbit();
            if (b){
                leading=utils::BitOperations::set(leading,i);
            }else{
                leading=utils::BitOperations::clr(leading,i);
            }
        }

        uint8_t tail = 0;
        for (int i = 5; i >= 0; i--) {
            auto b=_bw.getbit();
            _bw.incbit();
            if (b){
                tail=utils::BitOperations::set(tail,i);
            }else{
                tail=utils::BitOperations::clr(tail,i);
            }
        }
        timedb::Value result=0;
        for (int i = 63 - leading; i >= tail; i--) {
            auto b=_bw.getbit();
            _bw.incbit();
            if (b){
                result=utils::BitOperations::set(result,i);
            }else{
                result=utils::BitOperations::clr(result,i);
            }
        }

        _prev_lead = leading;
        _prev_tail = tail;
        auto ret= result ^ _prev_value;
        _prev_value=ret;
        return ret;
    }else{
        timedb::Value result = 0;

        for (int i = int(63 - _prev_lead); i >= _prev_tail; i--) {
            auto b=_bw.getbit();
            _bw.incbit();
            if (b){
                result=utils::BitOperations::set(result,i);
            }else{
                result=utils::BitOperations::clr(result,i);
            }
        }
        auto ret= result ^ _prev_value;
        _prev_value=ret;
        return ret;
    }
}

