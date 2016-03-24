#pragma once

#include "utils.h"
#include "storage/chunk.h"

namespace dariadb{
    namespace storage{
		
        class PageManager:public utils::NonCopy {
			~PageManager();
			PageManager(size_t chunk_per_storage, size_t chunk_size);
        public:
			typedef uint32_t handle;
            static void start(size_t chunk_per_storage,size_t chunk_size);
            static void stop();
            static PageManager* instance();

			uint64_t calc_page_size();

			bool append_chunk(const Chunk_Ptr&ch);
        private:
            static PageManager*_instance;
			size_t _chunk_per_storage;
			size_t _chunk_size;
        };
    }
}
