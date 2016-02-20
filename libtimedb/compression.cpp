#include "compression.h"
#include "utils.h"

using namespace timedb::compression;

const uint16_t delta_64_mask = 512;         //10 0000 0000
const uint16_t delta_256_mask = 3072;       //1100 0000 0000
const uint16_t delta_2047_mask = 57344;     //1110 0000 0000 0000
const uint64_t delta_big_mask = 64424509440;//1111 [0000 0000] [0000 0000][0000 0000] [0000 0000]

DeltaCompressor::DeltaCompressor()
{
}


DeltaCompressor::~DeltaCompressor()
{
}


uint16_t DeltaCompressor::get_delta_64(uint64_t D) {
	return delta_64_mask | static_cast<uint16_t>(D);
}

uint16_t DeltaCompressor::get_delta_256(uint64_t D) {
	return delta_256_mask| static_cast<uint16_t>(D);
}

uint16_t DeltaCompressor::get_delta_2048(uint64_t D) {
	return delta_2047_mask | static_cast<uint16_t>(D);
}


uint64_t DeltaCompressor::get_delta_big(uint64_t D) {
	return delta_big_mask | D;
}


BinaryWriter::BinaryWriter(uint8_t* begin,uint8_t*end):
    _begin(begin),
    _end(end),
    _cap(std::distance(begin,end)),
    _pos(0),
    _bitnum(max_bit_value){
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
}

BinaryWriter::~BinaryWriter(){
}

BinaryWriter& BinaryWriter::operator=(const BinaryWriter & other){
    BinaryWriter tmp(other);
	tmp.swap(*this);
	return *this;
}

void BinaryWriter::swap(BinaryWriter & other) throw(){
	std::swap(_pos, other._pos);
	std::swap(_cap, other._cap);
    std::swap(_begin, other._begin);
    std::swap(_end, other._end);
	std::swap(_bitnum, other._bitnum);
}

void BinaryWriter::incbit(){
	_bitnum--;
	if (_bitnum < 0) {
		incpos();
		_bitnum = max_bit_value;
	}
}

void BinaryWriter::incpos(){
	_pos++;
}

void BinaryWriter::set_bitnum(size_t num) {
	this->_bitnum = num;
}

void BinaryWriter::set_pos(size_t pos) {
	this->_pos = pos;
}

void BinaryWriter::reset_pos() {
	this->set_pos(0); 
	this->set_bitnum(max_bit_value);
}

uint8_t BinaryWriter::getbit() const{
    return (_begin[_pos] >> _bitnum) & 1;
}


void BinaryWriter::setbit() {
    _begin[_pos] |= (1 << _bitnum);
}

void BinaryWriter::clrbit() {
    _begin[_pos] &= ~(1 << _bitnum);
}
