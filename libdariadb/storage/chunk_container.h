#pragma once

#include "../meas.h"
#include "chunk.h"
#include "cursor.h"
#include "mode.h"
namespace dariadb {
	namespace storage {
        class ChunkWriter
        {
        public:
            virtual bool append(const Chunk_Ptr&c)=0;
            virtual bool append(const ChunksList&lst)=0;
        };

		class ChunkContainer
		{
		public:
			virtual bool minMaxTime(dariadb::Id id, dariadb::Time*minResult, dariadb::Time*maxResult) = 0;
			virtual Cursor_ptr chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to) = 0;
			virtual IdToChunkMap chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint) = 0;
			virtual IdArray getIds() = 0;
			virtual ~ChunkContainer();
		};
	}
}
