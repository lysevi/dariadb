#include "compression.h"
#include "utils.h"
#include "exception.h"
#include "binarybuffer.h"

#include <sstream>
#include<cassert>
#include <limits>

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


DeltaCompressor::DeltaCompressor(const DeltaCompressor &other):
    _is_first(other._is_first),
    _bw(other._bw),
    _first(other._first),
    _prev_delta(other._prev_delta),
    _prev_time(other._prev_time)
{}

DeltaCompressor::DeltaCompressor(const DeltaCompressor &&other):
    _is_first(other._is_first),
    _bw(std::move(other._bw)),
    _first(other._first),
    _prev_delta(other._prev_delta),
    _prev_time(other._prev_time)
{}

void DeltaCompressor::swap(DeltaCompressor &other){
    std::swap(_is_first,other._is_first);
    std::swap(_bw,other._bw);
    std::swap(_first,other._first);
    std::swap(_prev_delta,other._prev_delta);
    std::swap(_prev_time,other._prev_time);
}

DeltaCompressor& DeltaCompressor::operator=(DeltaCompressor &other){
    if(this==&other){
        return *this;
    }
    DeltaCompressor tmp(other);
    this->swap(tmp);
    return *this;
}

DeltaCompressor& DeltaCompressor::operator=(DeltaCompressor &&other){
    if(this==&other){
        return *this;
    }
    DeltaCompressor tmp(std::move(other));
    this->swap(tmp);
    return *this;
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


DeltaDeCompressor::DeltaDeCompressor(const DeltaDeCompressor &other):
    _bw(other._bw),
    _prev_delta(other._prev_delta),
    _prev_time(other._prev_time)
{}


void DeltaDeCompressor::swap(DeltaDeCompressor &other){
    std::swap(_bw,other._bw);
    std::swap(_prev_delta,other._prev_delta);
    std::swap(_prev_time,other._prev_time);
}

DeltaDeCompressor& DeltaDeCompressor::operator=(DeltaDeCompressor &&other){
    if(this==&other){
        return *this;
    }
    DeltaDeCompressor tmp(other);
    this->swap(tmp);
    return *this;
}

DeltaDeCompressor& DeltaDeCompressor::operator=(DeltaDeCompressor &other){
    if(this==&other){
        return *this;
    }
    DeltaDeCompressor tmp(std::move(other));
    this->swap(tmp);
    return *this;
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

XorCompressor::XorCompressor(const BinaryBuffer &bw):
    _is_first(true),
    _bw(bw),
    _first(0),
    _prev_value(0)
{

}

XorCompressor::~XorCompressor(){

}


XorCompressor::XorCompressor(const XorCompressor &other):
    _is_first(other._is_first),
    _bw(other._bw),
    _first(other._first),
    _prev_value(other._prev_value)
{}


void XorCompressor::swap(XorCompressor &other){
    std::swap(_is_first,other._is_first);
    std::swap(_bw,other._bw);
    std::swap(_first,other._first);
    std::swap(_prev_value,other._prev_value);
}

XorCompressor& XorCompressor::operator=(XorCompressor &other){
    if(this==&other){
        return *this;
    }
    XorCompressor tmp(other);
    this->swap(tmp);
    return *this;
}

XorCompressor& XorCompressor::operator=(XorCompressor &&other){
    if(this==&other){
        return *this;
    }
    XorCompressor tmp(std::move(other));
    this->swap(tmp);
    return *this;
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


XorDeCompressor::XorDeCompressor(const XorDeCompressor &other):
    _bw(other._bw),
    _prev_value(other._prev_value),
    _prev_lead(other._prev_lead),
    _prev_tail(other._prev_tail)
{}


void XorDeCompressor::swap(XorDeCompressor &other){
    std::swap(_bw,other._bw);
    std::swap(_prev_value,other._prev_value);
    std::swap(_prev_lead,other._prev_lead);
    std::swap(_prev_tail,other._prev_tail);
}

XorDeCompressor& XorDeCompressor::operator=(XorDeCompressor &other){
    if(this==&other){
        return *this;
    }

    XorDeCompressor tmp(other);
    this->swap(tmp);
    return *this;
}

XorDeCompressor& XorDeCompressor::operator=(XorDeCompressor &&other){
    if(this==&other){
        return *this;
    }

    XorDeCompressor tmp(std::move(other));
    this->swap(tmp);
    return *this;
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

CopmressedWriter::CopmressedWriter(BinaryBuffer bw_time, BinaryBuffer bw_values, BinaryBuffer bw_flags):
	time_comp(bw_time),
	value_comp(bw_values),
	flag_comp(bw_flags)
{
	_is_first = true;
	_is_full = false;
}

CopmressedWriter::~CopmressedWriter()
{}

CopmressedWriter::CopmressedWriter(const CopmressedWriter &other):

    time_comp(other.time_comp),
    value_comp(other.value_comp),
    flag_comp(other.flag_comp)

{
    _is_first=other._is_first;
    _is_full=other._is_full;
}

void CopmressedWriter::swap(CopmressedWriter &other){
    std::swap(time_comp,other.time_comp);
    std::swap(value_comp,other.value_comp);
    std::swap(flag_comp,other.flag_comp);
    std::swap(_is_first,other._is_first);
    std::swap(_is_full,other._is_full);
}

CopmressedWriter& CopmressedWriter::operator=(CopmressedWriter &other){
    CopmressedWriter temp(other);
    this->swap(other);
    return *this;
}

CopmressedWriter& CopmressedWriter::operator=(CopmressedWriter &&other){
    CopmressedWriter temp(std::move(other));
    this->swap(temp);
    return *this;
}

bool CopmressedWriter::append(const Meas&m) {
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

CopmressedReader::CopmressedReader(BinaryBuffer bw_time, BinaryBuffer bw_values, BinaryBuffer bw_flags, Meas first):
	time_dcomp(bw_time,first.time),
	value_dcomp(bw_values, first.value),
	flag_dcomp(bw_flags, first.flag)
{
	_first = first;
}

CopmressedReader::~CopmressedReader()
{
}

memseries::Meas CopmressedReader::read()
{
	Meas result{};
	result.time = time_dcomp.read();
	result.value = value_dcomp.read();
	result.flag = flag_dcomp.read();
	return result;
}
