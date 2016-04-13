#pragma once

#include "storage.h"
#include "storage/mode.h"
#include "storage/capacitor.h"
#include "storage/page_manager.h"
#include "storage/chunk_container.h"
#include "utils/utils.h"
#include <memory>

namespace dariadb {
	namespace storage {
		class UnionStorage :public BaseStorage{
		public:
			struct Limits {
				dariadb::Time old_mem_chunks; // old_mem_chunks - time when drop old chunks to page (MemStorage)
				size_t max_mem_chunks;        // max_mem_chunks - maximum chunks in memory.zero - by old_mem_chunks(MemStorage)
				
				Limits(const dariadb::Time old_time, const size_t max_mem) {
					old_mem_chunks = old_time;
					max_mem_chunks = max_mem;
				}
			};
			UnionStorage() = delete;
			UnionStorage(const UnionStorage&) = delete;
			UnionStorage&operator=(const UnionStorage&) = delete;
			virtual ~UnionStorage();


            ///
            /// \brief UnionStorage
            /// \param page_manager_params - params of page manager (PageManager)
            /// \param cap_params - capacitor params  (Capacitor)
            UnionStorage(storage::PageManager::Params page_manager_params,
					     dariadb::storage::Capacitor::Params cap_params,
						 const Limits&limits);

			Time minTime() override;
			Time maxTime() override;
			append_result append(const Meas::PMeas begin, const size_t size) override;
			append_result append(const Meas &value) override;
			void subscribe(const IdArray&ids, const Flag& flag, const ReaderClb_ptr &clbk) override;
			Reader_ptr currentValue(const IdArray&ids, const Flag& flag) override;
			void flush()override;

			Cursor_ptr chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to) override;
			IdToChunkMap chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint)override;
			IdArray getIds() override;

            size_t chunks_in_memory()const;
		protected:
			class Private;
			std::unique_ptr<Private> _impl;
		};
	}
}
