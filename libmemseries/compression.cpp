#include "compression.h"
#include "utils.h"
#include "exception.h"
#include <exception>
#include <sstream>
#include<cassert>

using namespace memseries::compression;

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

bool DeltaCompressor::append(memseries::Time t){
    if(_is_first){
        _first=t;
        _is_first=false;
        _prev_time=t;
        return true;
    }

    int64_t D=(t-_prev_time) - _prev_delta;
    if(D==0){
		if (_bw.free_size() == 1) {
			return false;
		}
        _bw.clrbit().incbit();
    }else{
        if ((-63<D)&&(D<64)){
			if (_bw.free_size() <2) {
				return false;
			}
            auto d=DeltaCompressor::get_delta_64(D);
            _bw.write(d,9);
        }else{
            if ((-255<D)&&(D<256)){
				if (_bw.free_size() <2) {
					return false;
				}
                auto d=DeltaCompressor::get_delta_256(D);
               _bw.write(d,11);
            }else{
                if ((-2047<D)&&(D<2048)){
					if (_bw.free_size() <3) {
						return false;
					}
                    auto d=DeltaCompressor::get_delta_2048(D);
                    _bw.write(d,15);
                }else{
                    if (_bw.free_size() <6) {
						return false;
					}
                    auto d=DeltaCompressor::get_delta_big(D);
                   _bw.write(d,35);
                }
            }
        }
    }

    _prev_delta=D;
    _prev_time=t;
	return true;
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

DeltaDeCompressor::DeltaDeCompressor(const BinaryBuffer &bw, memseries::Time first):
    _bw(bw),
    _prev_delta(0),
    _prev_time(first)
{

}

DeltaDeCompressor::~DeltaDeCompressor(){

}

memseries::Time DeltaDeCompressor::read(){
    auto b0=_bw.getbit();
    _bw.incbit();

    if(b0==0){
        return _prev_time+_prev_delta;
    }

    auto b1=_bw.getbit();
    _bw.incbit();
    if((b0==1) && (b1==0)){//64
        int8_t result=static_cast<int8_t>(_bw.read(7));

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
        int16_t result=static_cast<int16_t>(_bw.read(8));
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
        int16_t result=static_cast<int16_t>(_bw.read(11));
		if (result > 2048) { //is negative
			result = (-2048) | result;
		}
		
        auto ret=_prev_time+result+_prev_delta;
        _prev_delta=result;
        _prev_time=ret;
        return ret;
    }

    int64_t result=_bw.read(31);
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
    _bitnum(max_bit_pos)
{
    _cap=(std::distance(begin,end));
    _pos=_cap-1;
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
    _pos--;
    if (_pos==0){
		throw MAKE_EXCEPTION("BinaryBuffer::incpos");
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
    this->set_pos(_cap-1);
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

void BinaryBuffer::write(uint16_t v,int8_t count){
	uint32_t *dest = (uint32_t*)(_begin + _pos - 3);
	auto src = uint32_t(v) << (sizeof(uint16_t)*8 - count - 1);
	src = src << ((sizeof(uint16_t) * 8) - (max_bit_pos - _bitnum));
	*dest |= src;
	move_pos(count);	
}

void BinaryBuffer::write(uint64_t v, int8_t count) {
	assert(count < 47);
	uint64_t *dest = (uint64_t*)(_begin + _pos - 7);
	auto src = uint64_t(v) << ((sizeof(uint64_t) * 8 - count - 1) - (max_bit_pos - _bitnum));
	*dest |= src;
	move_pos(count);
}

uint64_t  BinaryBuffer::read(int8_t count) {
	uint64_t src = *(uint64_t*)(_begin + _pos - 7);
	src <<= (max_bit_pos - _bitnum);
	src >>= (sizeof(uint64_t) * 8 - count - 1);
	move_pos(count);
	return src;
}

std::ostream&  memseries::compression::operator<< (std::ostream& stream, const BinaryBuffer& b) {
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

bool XorCompressor::append(memseries::Value v){
	static_assert(sizeof(memseries::Value) == 8, "Value no x64 value");
	auto flat = inner::FlatDouble2Int(v);
    if(_is_first){
        _first= flat;
        _is_first=false;
        _prev_value= flat;
        return true;
    }
	if (_bw.free_size() <9) {
		return false;
	}
    auto xor_val=_prev_value^flat;
    if (xor_val==0){
		if (_bw.free_size() == 1) {
			return false;
		}
        _bw.clrbit().incbit();
        return true;
    }
    _bw.setbit().incbit();

    auto lead=zeros_lead(xor_val);
    auto tail=zeros_tail(xor_val);

	

    if ((_prev_lead==lead) && (_prev_tail==tail)){
        _bw.clrbit().incbit();
    }else{
        _bw.setbit().incbit();

        _bw.write((uint16_t)lead,int8_t(5));
        _bw.write((uint16_t)tail,int8_t(5));
    }

	xor_val = xor_val >> tail;
	_bw.write(xor_val, (63 - lead - tail));
    
    _prev_value = flat;
    _prev_lead=lead;
    _prev_tail=tail;
	return true;
}

uint8_t XorCompressor::zeros_lead(uint64_t v){
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

uint8_t XorCompressor::zeros_tail(uint64_t v){
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

XorDeCompressor::XorDeCompressor(const BinaryBuffer &bw, memseries::Value first):
    _bw(bw),
    _prev_value(inner::FlatDouble2Int(first)),
    _prev_lead(0),
    _prev_tail(0)
{

}

memseries::Value XorDeCompressor::read()
{
	static_assert(sizeof(memseries::Value) == 8, "Value no x64 value");
    auto b0=_bw.getbit();
    _bw.incbit();
    if(b0==0){
        return inner::FlatInt2Double(_prev_value);
    }

    auto b1=_bw.getbit();
    _bw.incbit();

    if(b1==1){
        uint8_t leading = static_cast<uint8_t>(_bw.read(5));

        uint8_t tail = static_cast<uint8_t>(_bw.read(5));
        uint64_t result=0;
		
		result=_bw.read(63 - leading - tail);
		result = result << tail;
        
        _prev_lead = leading;
        _prev_tail = tail;
        auto ret= result ^ _prev_value;
        _prev_value=ret;
        return inner::FlatInt2Double(ret);
    }else{
        uint64_t result = 0;

		result = _bw.read(63 - _prev_lead - _prev_tail);
		result = result << _prev_tail;

        auto ret= result ^ _prev_value;
        _prev_value=ret;
        return inner::FlatInt2Double(ret);
    }
}

FlagCompressor::FlagCompressor(const BinaryBuffer & bw):
	_bw(bw),
	_is_first(true),
	_first(0)
{
}

FlagCompressor::~FlagCompressor()
{
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

memseries::compression::FlagDeCompressor::FlagDeCompressor(const BinaryBuffer & bw, memseries::Flag first):
	_bw(bw),
	_prev_value(first){
}

memseries::Flag memseries::compression::FlagDeCompressor::read()
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

memseries::compression::CopmressedWriter::~CopmressedWriter()
{}

memseries::compression::CopmressedWriter::CopmressedWriter(BinaryBuffer bw_time, BinaryBuffer bw_values, BinaryBuffer bw_flags):
	time_comp(bw_time),
	value_comp(bw_values),
	flag_comp(bw_flags)
{
	_is_first = true;
	_is_full = false;
}

bool memseries::compression::CopmressedWriter::append(const Meas&m) {
	if (_is_first) {
		_first = m;
		_is_first = false;
	}
	
	if (_first.id != m.id) {
		std::stringstream ss{};
		ss << "(_first.id != m.id)" << " id:" << m.id << " first.id:" << _first.id;
		throw std::logic_error(ss.str().c_str());
	}
	if (time_comp.is_full() || value_comp.is_full() || flag_comp.is_full()) {
		_is_full = true;
		return false;
	}
	auto t_f = time_comp.append(m.time);
	auto f_f = value_comp.append(m.value);
	auto v_f = flag_comp.append(m.flag);

	if (!t_f || !f_f || !v_f) {
		_is_full = true;
		return false;
	}
	else {
		return true;
	}
}

memseries::compression::CopmressedReader::CopmressedReader(BinaryBuffer bw_time, BinaryBuffer bw_values, BinaryBuffer bw_flags, Meas first):
	time_dcomp(bw_time,first.time),
	value_dcomp(bw_values, first.value),
	flag_dcomp(bw_flags, first.flag)
{
	_first = first;
}

memseries::compression::CopmressedReader::~CopmressedReader()
{
}

memseries::Meas memseries::compression::CopmressedReader::read()
{
	Meas result{};
	result.time = time_dcomp.read();
	result.value = value_dcomp.read();
	result.flag = flag_dcomp.read();
	return result;
}
