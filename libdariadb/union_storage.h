#pragma once

#include "storage.h"
#include "storage/storage_mode.h"
#include "utils/utils.h"
#include <memory>

namespace dariadb {
	namespace storage {
		class UnionStorage :public AbstractStorage{
		public:
			UnionStorage() = delete;
			UnionStorage(const UnionStorage&) = delete;
			UnionStorage&operator=(const UnionStorage&) = delete;
			virtual ~UnionStorage();

			UnionStorage(const std::string &path, STORAGE_MODE mode, size_t chunk_per_storage, size_t chunk_size, const dariadb::Time write_window_deep, const size_t cap_max_size);
			Time minTime() override;
			Time maxTime() override;
			append_result append(const Meas::PMeas begin, const size_t size) override;
			append_result append(const Meas &value) override;
			Reader_ptr readInterval(const IdArray &ids, Flag flag, Time from, Time to) override;
			Reader_ptr readInTimePoint(const IdArray &ids, Flag flag, Time time_point) override;
			void subscribe(const IdArray&ids, const Flag& flag, const ReaderClb_ptr &clbk) override;
			Reader_ptr currentValue(const IdArray&ids, const Flag& flag) override;
		protected:
			class Private;
			std::unique_ptr<Private> _impl;
		};
	}
}