#pragma once
#include "binarybuffer.h"
#include "../meas.h"

namespace dariadb {
	namespace compression {
		class BaseCompressor {
		public:
            BaseCompressor(const BinaryBuffer_Ptr &bw);
            bool is_full() const { return _bw->is_full(); }
            size_t used_space()const { return _bw->cap() - _bw->pos(); }
		protected:
            BinaryBuffer_Ptr _bw;
		};
	}
}
