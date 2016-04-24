#pragma once

#include "../meas.h"
#include "chunk.h"
#include "cursor.h"
#include "mode.h"
namespace dariadb {
	namespace storage {
		class ChunkContainer
		{
		public:
            virtual bool append(const Chunk_Ptr&c)=0;
            virtual bool append(const ChuncksList&lst)=0;
			virtual Cursor_ptr chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to) = 0;
			virtual IdToChunkMap chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint) = 0;
			virtual IdArray getIds() = 0;
			virtual ~ChunkContainer();
		};
	}
}
