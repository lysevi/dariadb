#pragma once

#include "../meas.h"
#include "chunk.h"
#include "cursor.h"

namespace dariadb {
	namespace storage {
		class ChunkContainer
		{
		public:
			virtual Cursor_ptr chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to) = 0;
			virtual IdToChunkMap chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint) = 0;
			virtual IdArray getIds() = 0;
			virtual ~ChunkContainer();
		};
	}
}