#pragma once
#include "chunk.h"
#include "mode.h"
#include "cursor.h"
#include "../utils/fs.h"
#include "../utils/locker.h"

namespace dariadb {
	namespace storage {
#pragma pack(push, 1)
		struct PageHeader {
			uint32_t chunk_per_storage;
			uint32_t chunk_size;

			uint32_t pos_index;
			uint32_t pos_chunks;

			uint32_t count_readers;

			dariadb::Time minTime;
			dariadb::Time maxTime;
            uint64_t addeded_chunks;
            uint8_t  is_overwrite;
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
			~Page();
            bool append(const Chunk_Ptr&ch, MODE mode);
			bool is_full()const;
            uint32_t get_oldes_index();
			Cursor_ptr get_chunks(const dariadb::IdArray&ids, dariadb::Time from, dariadb::Time to, dariadb::Flag flag);
			ChuncksList get_open_chunks();
			void dec_reader();
		public:
			uint8_t        *region;
			PageHeader     *header;
			Page_ChunkIndex*index;
			uint8_t        *chunks;
		protected:
            dariadb::utils::Locker   _locker;
            utils::fs::MappedFile::MapperFile_ptr mmap;
		};

		class PageCursor : public dariadb::storage::Cursor{
		public:
			PageCursor(Page*page, const dariadb::IdArray&ids, dariadb::Time from, dariadb::Time to, dariadb::Flag flag);
			~PageCursor();
			bool is_end()const override;
			void readNext(Cursor::Callback*cbk)  override;
			void reset_pos() override;//start read from begining;
		protected:
			Page* link;
			bool _is_end;
			Page_ChunkIndex *_index_it, *_index_end;
			dariadb::IdArray _ids;
			dariadb::Time _from, _to;
			dariadb::Flag _flag;
			dariadb::utils::Locker _locker;
		};
	}
}
