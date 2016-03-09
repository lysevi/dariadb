#pragma once

#include "meas.h"
#include <memory>

namespace memseries {
    namespace storage {

        class Bucket {
        public:
            Bucket();
            ~Bucket();
            Bucket(const size_t max_size,const size_t count);
            Bucket(const Bucket&other);
            Bucket(Bucket&&other);
            void swap(Bucket&other) throw();
            Bucket& operator=(const Bucket&other);
            Bucket& operator=(Bucket&&other);

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
