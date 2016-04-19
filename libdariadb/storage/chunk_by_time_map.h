#pragma once

#include "chunk.h"
#include <stx/btree_multimap>

namespace dariadb{
    namespace storage{

        using stxMap=stx::btree_multimap<dariadb::Time, dariadb::storage::Chunk_Ptr>;
        class ChunkByTimeMap:public stxMap{
        public:
            void remove_droped();

            stxMap::const_iterator get_upper_bound(Time to)const;
            stxMap::iterator get_upper_bound(Time to);

            stxMap::const_iterator get_lower_bound(Time from)const;
            stxMap::iterator get_lower_bound(Time from);
        };
    }
}
