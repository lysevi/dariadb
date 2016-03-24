#pragma once

#include "../meas.h"
#include "../utils.h"
#include "../compression.h"
#include "../compression/binarybuffer.h"
#include <mutex>

namespace dariadb {
	namespace storage {
		struct Chunk
		{
		public:
			Chunk(size_t size, Meas first_m);
			~Chunk();
			bool append(const Meas&m);
			bool is_full()const { return c_writer.is_full(); }
		
			std::vector<uint8_t> _buffer_t;
			utils::Range range;
			compression::CopmressedWriter c_writer;
			size_t count;
			Meas first, last;

			Time minTime, maxTime;
			std::mutex _mutex;
			dariadb::Flag flag_bloom;
			compression::BinaryBuffer_Ptr bw;
		};

		typedef std::shared_ptr<Chunk>    Chunk_Ptr;
	}
}