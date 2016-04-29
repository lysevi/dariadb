#pragma once

#include "../meas.h"
#include "../utils/utils.h"
#include "../utils/locker.h"
#include "../utils/pool.h"
#include "../compression.h"
#include "../compression/binarybuffer.h"
#include <boost/lockfree/queue.hpp>

#include <map>
#include <set>
#include <unordered_map>
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
            typedef uint8_t* u8vector;
            Chunk(size_t size, Meas first_m);
            Chunk(const ChunkIndexInfo&index, const uint8_t* buffer,const size_t buffer_length);
            ~Chunk();

            bool append(const Meas&m);
            bool is_full()const { return c_writer.is_full(); }
            bool check_flag(const Flag& f);
            void lock() { _locker.lock(); }
            void unlock() { _locker.unlock(); }
            u8vector _buffer_t;
            size_t   _size;
            utils::Range range;
            compression::CopmressedWriter c_writer;

            utils::Locker _locker;
            compression::BinaryBuffer_Ptr bw;
            static void* operator new(std::size_t sz);
            static void operator delete(void* ptr, std::size_t sz);
        };

        typedef std::shared_ptr<Chunk>    Chunk_Ptr;
        typedef std::list<Chunk_Ptr>      ChunksList;
        typedef std::map<Id, Chunk_Ptr>   IdToChunkMap;
        typedef std::map<Id, ChunksList> ChunkMap;
		typedef std::unordered_map<Id, Chunk_Ptr>   IdToChunkUMap;

        const size_t ChunkPool_default_max_size = 100;

		//TODO need unit test.
        class ChunkPool:public utils::Pool{
        private:
            ChunkPool();
        public:
            ~ChunkPool();
            static void start();
            static void stop();
            static ChunkPool*instance();
        private:
            static std::unique_ptr<ChunkPool> _instance;
        };

        class ChunkBufferPool:public utils::Pool{
        private:
            ChunkBufferPool();
        public:
            ~ChunkBufferPool();
            static void start();
            static void stop();
            static ChunkBufferPool*instance();
        private:
            static std::unique_ptr<ChunkBufferPool> _instance;
        };

	}
}
