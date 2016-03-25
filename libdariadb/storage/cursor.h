#pragma once

#include "chunk.h"

namespace dariadb {
	namespace storage {
		struct Page;
		struct Page_ChunkIndex;
		class Cursor :public utils::NonCopy {
		public:
			Cursor(Page*page, const dariadb::IdArray&ids, dariadb::Time from, dariadb::Time to, dariadb::Flag flag);
			~Cursor();
			Cursor() = delete;
			bool is_end()const;
			Chunk_Ptr readNext();
			ChuncksList readAll();
			void reset_pos();//write to begining;
		protected:
			Page* link;
			bool _is_end;
			Page_ChunkIndex *_index_it, *_index_end;
			dariadb::IdArray _ids;
			dariadb::Time _from, _to;
			dariadb::Flag _flag;
		};

		typedef std::shared_ptr<Cursor> Cursor_ptr;
	}
}