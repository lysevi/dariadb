#pragma once

#include <libdariadb/compression/base_compressor.h>
#include <libdariadb/st_exports.h>

namespace dariadb {
namespace compression {
struct DeltaCompressor : public BaseCompressor {
  EXPORT DeltaCompressor(const ByteBuffer_Ptr &bw);

  EXPORT bool append(Time t);

  bool is_first;
  Time first;
  int64_t prev_delta;
  Time prev_time;
};

struct DeltaDeCompressor : public BaseCompressor {
  EXPORT DeltaDeCompressor(const ByteBuffer_Ptr &bw, Time first);

  EXPORT Time read();

  int64_t prev_delta;
  Time prev_time;
};
}
}
