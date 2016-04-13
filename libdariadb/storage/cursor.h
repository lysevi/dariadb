#pragma once

#include "../utils/locker.h"
#include "../utils/utils.h"
#include "chunk.h"

namespace dariadb {
	namespace storage {
		
		class Cursor :public utils::NonCopy {
		public:
            class Callback{
              public:
                virtual void call(dariadb::storage::Chunk_Ptr &ptr)=0;
				virtual ~Callback() = default;
            };
			virtual ~Cursor();
			virtual bool is_end()const=0;
			virtual void readNext(Callback*cbk)=0;
			virtual void reset_pos() = 0;//start read from begining;
			void readAll(Callback*cbk);
			void readAll(ChuncksList*output);
			
		};

		typedef std::shared_ptr<Cursor> Cursor_ptr;
		typedef std::list<Cursor_ptr>   CursorList;
	}
}
