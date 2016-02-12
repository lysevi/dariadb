#pragma once

#include "meas.h"

namespace timedb {

	class Compression
	{
	public:
		Compression();
		~Compression();

		static uint16_t compress_delta_64(uint64_t D);
		static uint16_t compress_delta_256(uint64_t D);
		static uint16_t compress_delta_2048(uint64_t D);
		static uint64_t compress_delta_big(uint64_t D);
	};

}