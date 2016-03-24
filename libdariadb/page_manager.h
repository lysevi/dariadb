#pragma once

#include "utils.h"
#include "storage/chunk.h"

#include <vector>

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

			
			bool append_chunk(const Chunk_Ptr&ch);
			//std::vector<handle> get_pages(const IdArray&ids, Time from, Time to, Flag flag);
        private:
            static PageManager*_instance;
			class Private;
			std::unique_ptr<Private> impl;
			
        };
    }
}
