#pragma once

#include "storage.h"
#include "storage/storage_mode.h"
#include "utils/utils.h"
#include <memory>

namespace dariadb {
	namespace storage {
		class UnionStorage :public BaseStorage{
		public:
			UnionStorage() = delete;
			UnionStorage(const UnionStorage&) = delete;
			UnionStorage&operator=(const UnionStorage&) = delete;
			virtual ~UnionStorage();


            ///
            /// \brief UnionStorage
            /// \param path - path to storage (PageManager)
            /// \param mode - storage mode  (PageManager)
            /// \param chunk_per_storage  - chunks count in page(PageManager)
            /// \param chunk_size - size of chunks in byte  (PageManager)
            /// \param write_window_deep - how long in past we can write (Capacitor)
            /// \param cap_max_size - max capacitor size  (Capacitor)
            /// \param old_mem_chunks - time when drop old chunks to page (MemStorage)
            UnionStorage(const std::string &path,
                         STORAGE_MODE mode, size_t chunk_per_storage, size_t chunk_size,
                         const dariadb::Time write_window_deep, const size_t cap_max_size,
                         const dariadb::Time old_mem_chunks);

			Time minTime() override;
			Time maxTime() override;
			append_result append(const Meas::PMeas begin, const size_t size) override;
			append_result append(const Meas &value) override;
			void subscribe(const IdArray&ids, const Flag& flag, const ReaderClb_ptr &clbk) override;
			Reader_ptr currentValue(const IdArray&ids, const Flag& flag) override;
			void flush()override;

			ChuncksList chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to) override;
			IdToChunkMap chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint)override;
			IdArray getIds()const override;
		protected:
			class Private;
			std::unique_ptr<Private> _impl;
		};
	}
}
