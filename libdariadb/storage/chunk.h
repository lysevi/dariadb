#pragma once

#include "../meas.h"
#include "../utils.h"
#include "../compression.h"
#include "../compression/binarybuffer.h"
#include <mutex>

namespace dariadb {
	namespace storage {
		struct ChunkIndexInfo {
			Meas first, last;
			size_t count;
			Time minTime, maxTime;
			dariadb::Flag flag_bloom;
		};

		struct Chunk:public ChunkIndexInfo
		{
		public:
			Chunk(size_t size, Meas first_m);
			~Chunk();
			bool append(const Meas&m);
			bool is_full()const { return c_writer.is_full(); }
			bool check_flag(const Flag& f);
			std::vector<uint8_t> _buffer_t;
			utils::Range range;
			compression::CopmressedWriter c_writer;
		
			std::mutex _mutex;			
			compression::BinaryBuffer_Ptr bw;
		};

		typedef std::shared_ptr<Chunk>    Chunk_Ptr;
	}
}