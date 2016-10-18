#pragma once

#include <libdariadb/compression/base_compressor.h>
#include <libdariadb/st_exports.h>
namespace dariadb {
namespace compression {

class IdCompressor : public BaseCompressor {
public:
  EXPORT IdCompressor(const BinaryBuffer_Ptr &bw);

  EXPORT bool append(Id v);

protected:
  bool _is_first;
  Id _first;
};

class IdDeCompressor : public BaseCompressor {
public:
  EXPORT IdDeCompressor(const BinaryBuffer_Ptr &bw, Id first);

  EXPORT Id read();

protected:
  Id _prev_value;
};
}
}
