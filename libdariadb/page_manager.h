#pragma once

#include "utils.h"
#include "storage/chunk.h"
#include "storage/storage_mode.h"
#include "storage/cursor.h"

#include <vector>

namespace dariadb{
    namespace storage{

        class PageManager:public utils::NonCopy {
			~PageManager();
            PageManager(const std::string &path, STORAGE_MODE mode, size_t chunk_per_storage, size_t chunk_size);
        public:

			typedef uint32_t handle;
            static void start(const std::string &path, STORAGE_MODE mode,size_t chunk_per_storage,size_t chunk_size);
            static void stop();
            static PageManager* instance();

			
			bool append_chunk(const Chunk_Ptr&ch);
			Cursor_ptr get_chunks(const IdArray&ids, Time from, Time to, Flag flag);
        private:
            static PageManager*_instance;
			class Private;
			std::unique_ptr<Private> impl;
        };
    }
}
