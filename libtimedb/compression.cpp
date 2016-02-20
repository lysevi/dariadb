#include "compression.h"
#include "utils.h"

using namespace timedb::compression;

const uint16_t delta_64_mask = 512;         //10 0000 0000
const uint16_t delta_256_mask = 3072;       //1100 0000 0000
const uint16_t delta_2047_mask = 57344;     //1110 0000 0000 0000
const uint64_t delta_big_mask = 64424509440;//1111 [0000 0000] [0000 0000][0000 0000] [0000 0000]

const uint8_t max_bit_value = 7;

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

BinaryBuffer::BinaryBuffer(size_t size):_cap(size), _pos(0),_bitnum(max_bit_value){
	this->_buf = new value_type[_cap];
	std::fill_n(_buf, _cap, value_type());
}

BinaryBuffer::BinaryBuffer(const BinaryBuffer & other) {
	this->_buf = new uint8_t[other._cap];
	_pos = other._pos;
	_bitnum = other._bitnum;
	_cap = other._cap;
	memcpy(_buf, other._buf, _pos);
}

BinaryBuffer::BinaryBuffer(BinaryBuffer && other){
	_buf = other._buf;
	_pos = other._pos;
	_cap = other._cap;
	_bitnum = other._bitnum;
	other._buf = nullptr;
	other._pos = 0;
	other._cap = 0;
	other._bitnum = 0;
}

BinaryBuffer::~BinaryBuffer(){
	if (_buf != nullptr) {
		delete[] _buf;
	}
}

BinaryBuffer& BinaryBuffer::operator=(const BinaryBuffer & other){
	BinaryBuffer tmp(other);
	tmp.swap(*this);
	return *this;
}

void BinaryBuffer::swap(BinaryBuffer & other) throw(){
	std::swap(_pos, other._pos);
	std::swap(_cap, other._cap);
	std::swap(_buf, other._buf);
	std::swap(_bitnum, other._bitnum);
}

void BinaryBuffer::incbit(){
	_bitnum--;
	if (_bitnum < 0) {
		incpos();
		_bitnum = max_bit_value;
	}
}

void BinaryBuffer::incpos(){
	_pos++;
}

void BinaryBuffer::set_bitnum(size_t num) {
	this->_bitnum = num;
}

void BinaryBuffer::set_pos(size_t pos) {
	this->_pos = pos;
}

void BinaryBuffer::reset_pos() { 
	this->set_pos(0); 
	this->set_bitnum(max_bit_value);
}

uint8_t BinaryBuffer::getbit() const{
	return (_buf[_pos] >> _bitnum) & 1;
}


void BinaryBuffer::setbit() {
	auto val = _buf[_pos] | (1 << _bitnum);
	_buf[_pos] = val;
}

void BinaryBuffer::clrbit() {
	_buf[_pos] &= ~(1 << _bitnum);
}