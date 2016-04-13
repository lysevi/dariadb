#pragma once

#include "../utils/utils.h"
#include "chunk.h"
#include "chunk_container.h"
#include "mode.h"
#include "cursor.h"

#include <vector>

namespace dariadb{
    namespace storage{

        class PageManager:public utils::NonCopy, public ChunkContainer {
		public:
			struct Params {
				std::string path;
				MODE mode;
				uint32_t chunk_per_storage;
				uint32_t chunk_size;
				Params(const std::string storage_path, MODE write_mode, size_t chunks_per_storage, size_t one_chunk_size) {
					path = storage_path;
					mode = write_mode;
					chunk_per_storage = uint32_t(chunks_per_storage);
					chunk_size = uint32_t(one_chunk_size);
				}
			};
		protected:
            virtual ~PageManager();
		
            PageManager(const Params&param);
        public:
			
			typedef uint32_t handle;
            static void start(const Params&param);
            static void stop();
            static PageManager* instance();
			
			bool append_chunk(const Chunk_Ptr&ch);
		
			/*/// thread unsafe method
			Cursor_ptr get_chunks(const IdArray&ids, Time from, Time to, Flag flag);*/

			//ChunkContainer
            Cursor_ptr chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to)override;
            IdToChunkMap chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint)override;
            IdArray getIds() override;
			
			dariadb::storage::ChuncksList get_open_chunks();
			size_t chunks_in_cur_page()const;

            dariadb::Time minTime();
            dariadb::Time maxTime();
		private:
            static PageManager*_instance;
			class Private;
			std::unique_ptr<Private> impl;
        };
    }
}
