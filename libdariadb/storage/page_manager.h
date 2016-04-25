#pragma once

#include "../utils/utils.h"
#include "chunk.h"
#include "chunk_container.h"
#include "mode.h"
#include "cursor.h"

#include <vector>

namespace dariadb{
    namespace storage{

        class PageManager:public utils::NonCopy, public ChunkContainer, public ChunkWriter {
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
			void flush();
			static PageManager* instance();

            bool append(const Chunk_Ptr&c)override;
            bool append(const ChunksList&lst)override;
			
			//ChunkContainer
            Cursor_ptr chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to)override;
            IdToChunkMap chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint)override;
            IdArray getIds() override;
			
			dariadb::storage::ChunksList get_open_chunks();
			size_t chunks_in_cur_page()const;
			size_t in_queue_size()const;

            dariadb::Time minTime();
            dariadb::Time maxTime();
		private:
            static PageManager*_instance;
			class Private;
			std::unique_ptr<Private> impl;
        };
    }
}
