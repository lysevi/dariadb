#pragma once

#include "libdariadb/compression/v2/base_compressor.h"

namespace dariadb {
namespace compression {
	namespace v2 {
		struct FlagCompressor : public BaseCompressor {
			FlagCompressor(const ByteBuffer_Ptr &bw);

			bool append(Flag v);
			Flag _first;
			bool _is_first;
		};

		struct FlagDeCompressor : public BaseCompressor {
			FlagDeCompressor(const ByteBuffer_Ptr &bw, Flag first);

			Flag read();
			Flag _first;
			bool _is_first;
		};
	}
	}
}
