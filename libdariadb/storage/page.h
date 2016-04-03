#pragma once
#include "chunk.h"
#include "mode.h"
#include "cursor.h"
#include "../utils/fs.h"

namespace dariadb {
	namespace storage {
#pragma pack(push, 1)
		struct PageHeader {
			//TODO replace indexes and pos to uint64_t. if needed.
			uint32_t chunk_per_storage;
			uint32_t chunk_size;

			uint32_t pos_index;
			uint32_t pos_chunks;

			uint32_t count_readers;

			dariadb::Time minTime;
			dariadb::Time maxTime;
             uint64_t addeded_chunks;
		};

		struct Page_ChunkIndex {
			ChunkIndexInfo info;
			uint64_t       offset;
            bool is_init;
		};
#pragma pack(pop) 

		class Page {
			Page() = default;
		public:
			static Page* create(std::string file_name, uint64_t sz, uint32_t chunk_per_storage, uint32_t chunk_size);
			static Page* open(std::string file_name);
			static PageHeader readHeader(std::string file_name);
			uint32_t get_oldes_index();
			~Page();
            bool append(const Chunk_Ptr&ch, MODE mode);
			bool is_full()const;
			Cursor_ptr get_chunks(const dariadb::IdArray&ids, dariadb::Time from, dariadb::Time to, dariadb::Flag flag);
			ChuncksList get_open_chunks();
			void dec_reader();
		public:
			uint8_t        *region;
			PageHeader     *header;
			Page_ChunkIndex*index;
			uint8_t        *chunks;
		protected:
			std::mutex      lock;
			utils::fs::MappedFile::MapperFile_ptr mmap;
		};
	}
}
