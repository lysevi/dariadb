#pragma once

#include "meas.h"
#include <memory>

namespace memseries {
    namespace storage {

        class MemBucket {
        public:
            MemBucket();
            ~MemBucket();
            MemBucket(const size_t max_size,const size_t count);
            MemBucket(const MemBucket&other);
            MemBucket(MemBucket&&other);
            void swap(MemBucket&other) throw();
            MemBucket& operator=(const MemBucket&other);
            MemBucket& operator=(MemBucket&&other);

            bool append(const Meas&m);
            size_t size()const;
            size_t max_size()const;
            bool is_full()const;
            memseries::Time minTime()const;
            memseries::Time maxTime()const;
        protected:
            class Private;
            std::unique_ptr<Private> _Impl;
        };
    }
}
