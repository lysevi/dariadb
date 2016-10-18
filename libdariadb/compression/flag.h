#pragma once

#include <libdariadb/compression/base_compressor.h>
#include <libdariadb/st_exports.h>

namespace dariadb {
namespace compression {

class FlagCompressor : public BaseCompressor {
public:
  EXPORT FlagCompressor(const BinaryBuffer_Ptr &bw);

  EXPORT bool append(Flag v);

protected:
  bool _is_first;
  Flag _first;
};

class FlagDeCompressor : public BaseCompressor {
public:
  EXPORT FlagDeCompressor(const BinaryBuffer_Ptr &bw, Flag first);
  EXPORT Flag read();
protected:
  Flag _prev_value;
};
}
}
