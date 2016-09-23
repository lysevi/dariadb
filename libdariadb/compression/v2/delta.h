#pragma once

#include "base_compressor.h"

namespace dariadb {
namespace compression {
namespace v2 {

struct DeltaCompressor : public BaseCompressor {
  DeltaCompressor(const ByteBuffer_Ptr &bw);

  bool append(Time t);

  bool is_first;
  Time first;
  int64_t prev_delta;
  Time prev_time;
};

struct DeltaDeCompressor : public BaseCompressor {
  DeltaDeCompressor(const ByteBuffer_Ptr &bw, Time first);

  Time read();

  int64_t prev_delta;
  Time prev_time;
};
}
}
}
