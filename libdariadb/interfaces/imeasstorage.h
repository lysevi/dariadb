#pragma once

#include "imeaswriter.h"
#include "imeassource.h"
#include "../utils/utils.h"

namespace dariadb {
	namespace storage {

		class IMeasStorage : public utils::NonCopy, public IMeasSource, public IMeasWriter {
		public:
		};

		typedef std::shared_ptr<IMeasStorage> IMeasStorage_ptr;
	}
}
