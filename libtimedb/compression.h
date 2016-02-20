#pragma once

#include "meas.h"

namespace timedb {

	class DeltaCompressor
	{
	public:
		DeltaCompressor();
		~DeltaCompressor();

		static uint16_t get_delta_64(uint64_t D);
		static uint16_t get_delta_256(uint64_t D);
		static uint16_t get_delta_2048(uint64_t D);
		static uint64_t get_delta_big(uint64_t D);
	};

}