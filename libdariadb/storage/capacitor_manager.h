#pragma once

#include "../storage.h"
#include "../utils/utils.h"
#include "chunk_container.h"
#include "capacitor.h"
#include <vector>

namespace dariadb {
	namespace storage {

		class CapacitorManager : public MeasStorage {
		public:
			struct Params {
				std::string path;
				size_t max_levels;
				size_t B; // measurements count in one datra block
				Params() {
					max_levels = 0;
					B = 0;
				}
				Params(const std::string storage_path, const size_t _B) {
					path = storage_path;
					B = _B;
					max_levels = 0;
				}
			};

		protected:
			virtual ~CapacitorManager();

			CapacitorManager(const Params &param);

		public:
			static void start(const Params &param);
			static void stop();
			static CapacitorManager *instance();

			// Inherited via MeasStorage
			virtual Time minTime() override;
			virtual Time maxTime() override;
			virtual bool minMaxTime(dariadb::Id id, dariadb::Time * minResult, dariadb::Time * maxResult) override;
			virtual Reader_ptr readInterval(const QueryInterval & q) override;
			virtual Reader_ptr readInTimePoint(const QueryTimePoint & q) override;
			virtual Reader_ptr currentValue(const IdArray & ids, const Flag & flag) override;
			virtual append_result append(const Meas & value) override;
			virtual void flush() override;
			virtual void subscribe(const IdArray & ids, const Flag & flag, const ReaderClb_ptr & clbk) override;
        protected:
            void create_new();
		private:
			static CapacitorManager *_instance;

			Params _params;
			Capacitor_Ptr _cap;
			
		};
	}
}
