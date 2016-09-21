#pragma once

#include "base_compressor.h"

namespace dariadb {
namespace compression {
namespace v2 {

struct DeltaCompressor : public BaseCompressor {
  DeltaCompressor(const ByteBuffer_Ptr &bw);

  bool append(Time t);

  bool _is_first;
  Time _first;
  int64_t _prev_delta;
  Time _prev_time;
};

struct DeltaDeCompressor : public BaseCompressor {
  DeltaDeCompressor(const ByteBuffer_Ptr &bw, Time first);

  Time read();

  int64_t _prev_delta;
  Time _prev_time;
};
}
}
}
