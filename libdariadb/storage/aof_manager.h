#pragma once

#include "../storage.h"
#include "../utils/utils.h"
#include "aofile.h"
#include <vector>

#include <mutex>

namespace dariadb {
	namespace storage {

        class AOFManager : public MeasStorage {
		public:
			struct Params {
				std::string path;
                size_t max_size; // measurements count in one datra block
				Params() {
                    max_size = 0;
				}
                Params(const std::string storage_path, const size_t _max_size) {
					path = storage_path;
                    max_size = _max_size;
				}
			};

		protected:
            virtual ~AOFManager();

            AOFManager(const Params &param);

		public:
			static void start(const Params &param);
			static void stop();
            static AOFManager *instance();

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

            std::list<std::string> closed_caps();
            void drop_aof(const std::string&fname, MeasWriter* storage);

			size_t files_count() const;
            void set_downlevel(MeasWriter* down){_down=down;}
        protected:
            void create_new();
            std::list<std::string> aof_files()const;
		private:
            static AOFManager *_instance;

			Params _params;
            AOFile_Ptr _cap;
            mutable std::mutex _locker;
            MeasWriter* _down;
		};
	}
}
