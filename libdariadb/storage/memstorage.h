#pragma once

#include "../storage.h"
#include "chunk.h"

namespace dariadb {
    namespace storage {

        class MemoryStorage : public BaseStorage, public ChunkWriter{
        public:
            MemoryStorage(size_t size);
            virtual ~MemoryStorage();

            using BaseStorage::append;
            using BaseStorage::readInterval;
            using BaseStorage::readInTimePoint;

            Time minTime() override;
            Time maxTime() override;
            append_result append(const Meas &value) override;
            append_result append(const Meas::PMeas begin, const size_t size) override;

            void subscribe(const IdArray&ids,const Flag& flag, const ReaderClb_ptr &clbk)override;
			Reader_ptr currentValue(const IdArray&ids, const Flag& flag)override;
			void flush()override;

            size_t size() const;
            size_t chunks_size() const;
            size_t chunks_total_size()const;

			//drop old fulled chunks.
			ChunksList drop_old_chunks(const dariadb::Time min_time);
			ChunksList drop_old_chunks_by_limit(const size_t max_limit);
			dariadb::storage::ChunksList drop_all();
			//ChunkContainer
			bool minMaxTime(dariadb::Id id, dariadb::Time*minResult, dariadb::Time*maxResult)override;
			Cursor_ptr chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to)override;
			IdToChunkMap chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint)override;
			IdArray getIds() override;

            bool append(const ChunksList&clist)override;
            bool append(const Chunk_Ptr&c)override;

			size_t queue_size()const;

            void set_chunkWriter(ChunkWriter*cw);
        protected:
            class Private;
            std::unique_ptr<Private> _Impl;
        };

    }
}
