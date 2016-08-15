#pragma once

#include "base_compressor.h"

namespace dariadb {
namespace compression {

class IdCompressor : public BaseCompressor {
public:
  IdCompressor(const BinaryBuffer_Ptr &bw);

  bool append(Id v);

protected:
  bool _is_first;
  Id _first;
};

class IdDeCompressor : public BaseCompressor {
public:
  IdDeCompressor(const BinaryBuffer_Ptr &bw, Id first);

  Id read();

protected:
  Id _prev_value;
};
}
}
