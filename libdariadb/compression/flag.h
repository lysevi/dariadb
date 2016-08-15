#pragma once

#include "base_compressor.h"

namespace dariadb {
namespace compression {

class FlagCompressor : public BaseCompressor {
public:
  FlagCompressor(const BinaryBuffer_Ptr &bw);

  bool append(Flag v);
protected:
  bool _is_first;
  Flag _first;
};

class FlagDeCompressor : public BaseCompressor {
public:
  FlagDeCompressor(const BinaryBuffer_Ptr &bw, Flag first);

  Flag read();

protected:
  Flag _prev_value;
};
}
}
