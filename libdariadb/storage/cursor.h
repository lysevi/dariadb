#pragma once

#include "chunk.h"
#include "../utils/spinlock.h"

namespace dariadb {
	namespace storage {
		class Page;
		struct Page_ChunkIndex;
		class Cursor :public utils::NonCopy {
		public:
            class Callback{
              public:
                virtual void call(Chunk_Ptr &ptr)=0;
				virtual ~Callback() = default;
            };
			Cursor(Page*page, const dariadb::IdArray&ids, dariadb::Time from, dariadb::Time to, dariadb::Flag flag);
			~Cursor();
			Cursor() = delete;
			bool is_end()const;
            void readNext(Callback*cbk);
            void readAll(Callback*cbk);
            void readAll(ChuncksList*output);
			void reset_pos();//start read from begining;
		protected:
			Page* link;
			bool _is_end;
			Page_ChunkIndex *_index_it, *_index_end;
			dariadb::IdArray _ids;
			dariadb::Time _from, _to;
			dariadb::Flag _flag;
            dariadb::utils::SpinLock _locker;
		};

		typedef std::shared_ptr<Cursor> Cursor_ptr;
	}
}
