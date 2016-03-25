#pragma once
#include "chunk.h"
#include "storage_mode.h"
#include "cursor.h"

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
		};

		struct Page_ChunkIndex {
			ChunkIndexInfo info;
			uint64_t       offset;
		};
#pragma pack(pop)

		struct Page {
			uint8_t        *region;
			PageHeader     *header;
			Page_ChunkIndex*index;
			uint8_t        *chunks;
			std::mutex      lock;

			uint32_t get_oldes_index();

			bool append(const Chunk_Ptr&ch, STORAGE_MODE mode);
			bool is_full()const;
			Cursor_ptr get_chunks(const dariadb::IdArray&ids, dariadb::Time from, dariadb::Time to, dariadb::Flag flag);
		};
	}
}