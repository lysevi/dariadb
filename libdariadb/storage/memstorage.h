#pragma once

#include "../storage.h"
#include "chunk.h"

namespace dariadb {
    namespace storage {

        class MemoryStorage : public AbstractStorage, public ChunkContainer {
        public:
            MemoryStorage(size_t size);
            virtual ~MemoryStorage();

            using AbstractStorage::append;
            using AbstractStorage::readInterval;
            using AbstractStorage::readInTimePoint;

            Time minTime() override;
            Time maxTime() override;
            append_result append(const Meas &value) override;
            append_result append(const Meas::PMeas begin, const size_t size) override;
            Reader_ptr readInterval(const IdArray &ids, Flag flag, Time from,
                                    Time to) override;
            Reader_ptr readInTimePoint(const IdArray &ids, Flag flag,
                                       Time time_point) override;

            size_t size() const;
            size_t chunks_size() const;
			size_t chunks_total_size()const;
            void subscribe(const IdArray&ids,const Flag& flag, const ReaderClb_ptr &clbk)override;
			Reader_ptr currentValue(const IdArray&ids, const Flag& flag)override;
			void flush()override;

			ChuncksList drop_old_chunks(const dariadb::Time min_time);
			ChuncksList chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to)override;
        protected:
            class Private;
            std::unique_ptr<Private> _Impl;
        };
    }
}
