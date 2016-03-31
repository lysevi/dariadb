#pragma once

#include "../meas.h"
#include "../storage.h"
#include "../utils/period_worker.h"
#include <memory>

namespace dariadb {
    namespace storage {
		/// used as added period in PeriodWorker
		const dariadb::Time capasitor_sync_delta = 300;
        class Capacitor:public utils::NonCopy, protected dariadb::utils::PeriodWorker {
        public:
            ~Capacitor();
            Capacitor(const size_t max_size, const BaseStorage_ptr stor, const dariadb::Time write_window_deep);

            bool append(const Meas&m);
            size_t size()const;
            dariadb::Time minTime()const;
            dariadb::Time maxTime()const;
			size_t writed_count()const;
			bool flush();//write all to storage;
			void clear();
        protected:
            class Private;
            std::unique_ptr<Private> _Impl;

			// Inherited via PeriodWorker
			virtual void call() override;
		};
    }
}
