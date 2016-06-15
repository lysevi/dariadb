#pragma once

#include "../storage.h"
#include "../utils/utils.h"
#include "chunk_container.h"

#include <vector>

namespace dariadb {
	namespace storage {

		class CapacitorManager : public utils::NonCopy{
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
		private:
			static CapacitorManager *_instance;

			Params _params;
		};
	}
}