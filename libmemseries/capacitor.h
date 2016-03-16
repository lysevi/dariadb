#pragma once

#include "meas.h"
#include "storage.h"
#include <memory>

namespace memseries {
    namespace storage {

        class Capacitor {
        public:
            Capacitor();
            ~Capacitor();
            Capacitor(const size_t max_size, const AbstractStorage_ptr stor, const memseries::Time write_window_deep);
            Capacitor(const Capacitor&other);
            Capacitor(Capacitor&&other);
            void swap(Capacitor&other) throw();
            Capacitor& operator=(const Capacitor&other);
            Capacitor& operator=(Capacitor&&other);

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
