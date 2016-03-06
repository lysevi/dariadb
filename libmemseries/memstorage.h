#pragma once

#include "storage.h"

namespace memseries{
    namespace storage {

        class MemoryStorage:public AbstractStorage
        {
        public:
            MemoryStorage(size_t size);
            virtual ~MemoryStorage();

            using AbstractStorage::append;
            using AbstractStorage::readInterval;
            using AbstractStorage::readInTimePoint;

            Time minTime()override;
            Time maxTime()override;
            append_result append(const Meas& value)override;
            append_result append(const Meas::PMeas begin, const size_t size)override;
            Reader_ptr readInterval(const IdArray &ids, Flag flag, Time from, Time to)override;
            Reader_ptr readInTimePoint(const IdArray &ids, Flag flag, Time time_point)override;

	  size_t size()const;
	  size_t chunks_size()const;
         protected:
	  class Impl;
	  Impl*_Impl;
        };
    }
}
