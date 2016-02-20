#include "compression.h"
#include "utils.h"

using namespace timedb;

const uint16_t delta_64_mask = 512;         //10 0000 0000
const uint16_t delta_256_mask = 3072;       //1100 0000 0000
const uint16_t delta_2047_mask = 57344;     //1110 0000 0000 0000
const uint64_t delta_big_mask = 64424509440;//1111 [0000 0000] [0000 0000][0000 0000] [0000 0000]

Compression::Compression()
{
}


Compression::~Compression()
{
}


uint16_t Compression::compress_delta_64(uint64_t D) {
	return delta_64_mask | static_cast<uint16_t>(D);
}

uint16_t Compression::compress_delta_256(uint64_t D) {
	return delta_256_mask| static_cast<uint16_t>(D);
}

uint16_t Compression::compress_delta_2048(uint64_t D) {
	return delta_2047_mask | static_cast<uint16_t>(D);
}


uint64_t Compression::compress_delta_big(uint64_t D) {
	return delta_big_mask | D;
}