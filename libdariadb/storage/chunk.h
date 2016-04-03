#pragma once

#include "../meas.h"
#include "../utils/utils.h"
#include "../compression.h"
#include "../compression/binarybuffer.h"

#include <mutex>
#include <map>

namespace dariadb {
	namespace storage {

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

        class Chunk_Ptr{
        public:
            std::shared_ptr<Chunk> _shared_ptr;
            Chunk_Ptr():_shared_ptr{nullptr}
            {

            }
            Chunk_Ptr(Chunk*ptr):_shared_ptr{ptr}{

            }

            Chunk_Ptr(std::shared_ptr<Chunk> ptr):_shared_ptr{ptr}{

            }

            Chunk_Ptr& operator=(std::shared_ptr<Chunk> shared_ptr){
                _shared_ptr=shared_ptr;
                return *this;
            }

            Chunk_Ptr& operator=(const Chunk_Ptr &other){
                if(&other!=this){
                    _shared_ptr=other._shared_ptr;
                }
                return *this;
            }

            Chunk_Ptr& operator=(const std::nullptr_t &ptr){
                _shared_ptr=ptr;
                return *this;
            }

            bool operator==(const Chunk* ptr)const{
                return _shared_ptr.get()==ptr;
            }

            bool operator==(const Chunk_Ptr &ptr)const{
                return _shared_ptr==ptr._shared_ptr;
            }

            bool operator==(const std::nullptr_t &ptr)const{
                return _shared_ptr.get()==ptr;
            }

            bool operator!=(const Chunk* ptr)const{
                return _shared_ptr.get()!=ptr;
            }

            bool operator!=(const Chunk_Ptr &ptr)const{
                return _shared_ptr!=ptr._shared_ptr;
            }

            bool operator!=(const std::nullptr_t &ptr)const{
                return _shared_ptr.get()!=ptr;
            }

            std::shared_ptr<Chunk> operator ->(){
                return _shared_ptr;
            }

            const std::shared_ptr<Chunk> operator ->()const{
                return _shared_ptr;
            }

            Chunk* get(){
                return _shared_ptr.get();
            }

            const Chunk* get()const{
                return _shared_ptr.get();
            }
        };

        class ChunkPool{
            static ChunkPool *_instance;
            ChunkPool();
            ~ChunkPool();
        public:
            static void start();
            static void stop();
            static ChunkPool*instance();

            std::shared_ptr<Chunk> alloc();
            void free(std::shared_ptr<Chunk> ptr);
        protected:
            std::list<std::shared_ptr<Chunk>> _ptrs;
        };


		typedef std::list<Chunk_Ptr>      ChuncksList;
		typedef std::map<Id, Chunk_Ptr>   IdToChunkMap;
		typedef std::map<Id, ChuncksList> ChunkMap;

		class ChunkContainer
		{
		public:
			virtual ChuncksList chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to)=0;
			virtual IdToChunkMap chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint) = 0;
			virtual IdArray getIds()const=0;
		};
	}
}
