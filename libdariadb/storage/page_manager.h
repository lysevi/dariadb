#pragma once

#include "../utils/utils.h"
#include "chunk.h"
#include "mode.h"
#include "cursor.h"

#include <vector>

namespace dariadb{
    namespace storage{

        class PageManager:public utils::NonCopy, public ChunkContainer {
            virtual ~PageManager();
            PageManager(const std::string &path, MODE mode, size_t chunk_per_storage, size_t chunk_size);
        public:

			typedef uint32_t handle;
            static void start(const std::string &path, MODE mode,size_t chunk_per_storage,size_t chunk_size);
            static void stop();
            static PageManager* instance();
			
			bool append_chunk(const Chunk_Ptr&ch);
		
			/// thread unsafe method
			Cursor_ptr get_chunks(const IdArray&ids, Time from, Time to, Flag flag);

			//ChunkContainer
            ChuncksList chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to)override;
            IdToChunkMap chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint)override;
            IdArray getIds()const override;

		private:
            static PageManager*_instance;
			class Private;
			std::unique_ptr<Private> impl;
        };
    }
}