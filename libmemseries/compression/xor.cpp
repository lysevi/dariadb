#include "xor.h"
#include "binarybuffer.h"
#include "../utils.h"
#include <sstream>
#include<cassert>
#include <limits>

using namespace memseries;
using namespace memseries::compression;

XorCompressor::XorCompressor(const BinaryBuffer &bw):
	BaseCompressor(bw),
    _is_first(true),
    _first(0),
    _prev_value(0)
{}

XorCompressor::~XorCompressor(){
}


bool XorCompressor::append(Value v){
    static_assert(sizeof(Value) == 8, "Value no x64 value");
    auto flat = inner::flat_double_to_int(v);
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

XorDeCompressor::XorDeCompressor(const BinaryBuffer &bw, Value first):
	BaseCompressor(bw),
    _prev_value(inner::flat_double_to_int(first)),
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
        return inner::flat_int_to_double(_prev_value);
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
        return inner::flat_int_to_double(ret);
    }else{
        uint64_t result = 0;

        result = _bw.read(63 - _prev_lead - _prev_tail);
        result = result << _prev_tail;

        auto ret= result ^ _prev_value;
        _prev_value=ret;
        return inner::flat_int_to_double(ret);
    }
}
