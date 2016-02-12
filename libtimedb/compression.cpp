#include "compression.h"
#include "utils.h"

using namespace timedb;

union Uint16Union
{
	uint16_t all;

	struct {
		uint8_t low;
		uint8_t hight;
	}pair;
};

union Uint64Union
{
	uint64_t all;
	struct {
		uint8_t b0;
		uint8_t b1;
		uint8_t b2;
		uint8_t b3;
		uint8_t b4;
		uint8_t b5;
		uint8_t b6;
		uint8_t b7;
	}bytes;
};



Compression::Compression()
{
}


Compression::~Compression()
{
}


uint16_t Compression::compress_delta_64(uint64_t D) {
    //1000 0000 0
	Uint16Union res_union;
	Uint64Union u64;
	u64.all = D;

	res_union.all = 0;
	res_union.pair.hight = 1;
	res_union.pair.low = u64.bytes.b0;
	return res_union.all;
}

uint16_t Compression::compress_delta_256(uint64_t D) {
	//1100 0000 0000
	Uint16Union Dbytes;
	Dbytes.all = (uint16_t)D;

	Uint16Union res;
	res.all = 0;
	res.pair.hight = 12; // '110'
	
	auto b = utils::BitOperations::get(Dbytes.pair.hight, 0);
	res.pair.hight=utils::BitOperations::set(res.pair.hight, 0, b);
	res.pair.low = Dbytes.pair.low;
	return res.all;
}

uint16_t Compression::compress_delta_2048(uint64_t D) {
	Uint16Union Dbytes;
	Dbytes.all = (uint16_t)D;

	Uint16Union res;
	res.all = Dbytes.all;
	
	res.pair.hight = utils::BitOperations::set(res.pair.hight, 4, 0);
	res.pair.hight = utils::BitOperations::set(res.pair.hight, 5, 1);
	res.pair.hight = utils::BitOperations::set(res.pair.hight, 6, 1);
	res.pair.hight = utils::BitOperations::set(res.pair.hight, 7, 1);
	return res.all;
}


uint64_t Compression::compress_delta_big(uint64_t D) {
	Uint64Union res;
	res.all = D;
	res.bytes.b4 = 15;
	return res.all;
}