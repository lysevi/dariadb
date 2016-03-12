#pragma once

#include "meas.h"
#include "storage.h"
#include <memory>

namespace memseries {
    namespace storage {

        class Bucket {
        public:
            Bucket();
            ~Bucket();
            Bucket(const size_t max_size, const AbstractStorage_ptr stor, const memseries::Time write_window_deep);
            Bucket(const Bucket&other);
            Bucket(Bucket&&other);
            void swap(Bucket&other) throw();
            Bucket& operator=(const Bucket&other);
            Bucket& operator=(Bucket&&other);

            bool append(const Meas&m);
            size_t size()const;
            memseries::Time minTime()const;
            memseries::Time maxTime()const;
			size_t writed_count()const;
			bool flush();//write all to storage;
			void clear();
        protected:
            class Private;
            std::unique_ptr<Private> _Impl;
        };
    }
}
