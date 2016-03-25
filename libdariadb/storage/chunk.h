#pragma once

#include "../meas.h"
#include "../utils.h"
#include "../compression.h"
#include "../compression/binarybuffer.h"

#include <mutex>

namespace dariadb {
	namespace storage {

		struct ChunkIndexInfo {
			//!!! check ctor of Chunk when change this struct.
			Meas first, last;
			Time minTime, maxTime;
			dariadb::Flag flag_bloom;
			uint32_t count;
			size_t bw_pos;
			uint8_t  bw_bit_num;
			bool is_readonly;
            compression::CopmressedWriter::Position writer_position;
		};


		struct Chunk:public ChunkIndexInfo
		{
		public:
			Chunk(size_t size, Meas first_m);
			Chunk(const ChunkIndexInfo&index, const uint8_t* buffer,const size_t buffer_length);
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
		typedef std::list<Chunk_Ptr>      ChuncksList;
	}
}
