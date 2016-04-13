#pragma once

#include "../storage.h"
#include "chunk.h"

namespace dariadb {
    namespace storage {

        class MemoryStorage : public BaseStorage{
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
			ChuncksList drop_old_chunks(const dariadb::Time min_time);
			ChuncksList drop_old_chunks_by_limit(const size_t max_limit);
			dariadb::storage::ChuncksList drop_all();

			Cursor_ptr chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to)override;
			IdToChunkMap chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint)override;
			IdArray getIds() override;

			void add_chunks(const ChuncksList&clist);
        protected:
            class Private;
            std::unique_ptr<Private> _Impl;
        };

    }
}
