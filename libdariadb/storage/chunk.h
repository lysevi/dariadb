#pragma once

#include "../meas.h"
#include "../utils/utils.h"
#include "../utils/locker.h"
#include "../compression.h"
#include "../compression/binarybuffer.h"

#include <map>

namespace dariadb {
	namespace storage {

#pragma pack(push, 1)
		struct ChunkIndexInfo {
			//!!! check ctor of Chunk when change this struct.
			Meas first, last;
			Time minTime, maxTime;
			dariadb::Flag flag_bloom;
			uint32_t count;
			uint32_t bw_pos;
			uint8_t  bw_bit_num;
			bool is_readonly;
            compression::CopmressedWriter::Position writer_position;

            bool is_dropped;
		};
#pragma pack(pop)

        struct Chunk:public ChunkIndexInfo
        {
        public:
            Chunk(size_t size, Meas first_m);
            Chunk(const ChunkIndexInfo&index, const uint8_t* buffer,const size_t buffer_length);
            ~Chunk();

            bool append(const Meas&m);
            bool is_full()const { return c_writer.is_full(); }
            bool check_flag(const Flag& f);
            void lock() { _locker.lock(); }
            void unlock() { _locker.unlock(); }
            std::vector<uint8_t> _buffer_t;
            utils::Range range;
            compression::CopmressedWriter c_writer;

            utils::Locker _locker;
            compression::BinaryBuffer_Ptr bw;
            static void* operator new(std::size_t sz);
            static void operator delete(void* ptr, std::size_t sz);
        };

        typedef std::shared_ptr<Chunk>    Chunk_Ptr;
        typedef std::list<Chunk_Ptr>      ChuncksList;
        typedef std::map<Id, Chunk_Ptr>   IdToChunkMap;
        typedef std::map<Id, ChuncksList> ChunkMap;

		const size_t ChunkPool_default_max_size = 200;

        class ChunkPool{
        private:
            ChunkPool();
        public:
            ~ChunkPool();
            static void start(size_t max_size);
            static void stop();
            static ChunkPool*instance();

            void*alloc(std::size_t sz);
            void free(void* ptr, std::size_t sz);
            size_t polled();
        private:
            static std::unique_ptr<ChunkPool> _instance;
            std::list<void*> _ptrs;
			size_t _max_size;
            utils::Locker _locker;
        };
	}
}
