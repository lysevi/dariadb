#include "compression.h"
#include <bitset>

using namespace timedb;

union Delta64Union
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
	union 
	{
		uint16_t all;

		struct {
			uint8_t low;
			uint8_t hight;
		}pair;
	}res_union;
	Delta64Union u64;
	u64.all = D;

	res_union.all = 0;
	res_union.pair.hight = 1;
	res_union.pair.low = u64.bytes.b0;
	return res_union.all;
}