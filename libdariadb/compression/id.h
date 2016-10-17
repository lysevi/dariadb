#pragma once

#include <libdariadb/compression/base_compressor.h>
#include <libdariadb/dariadb_st_exports.h>
namespace dariadb {
namespace compression {

class IdCompressor : public BaseCompressor {
public:
  DARIADB_ST_EXPORTS IdCompressor(const BinaryBuffer_Ptr &bw);

  DARIADB_ST_EXPORTS bool append(Id v);

protected:
  bool _is_first;
  Id _first;
};

class IdDeCompressor : public BaseCompressor {
public:
  DARIADB_ST_EXPORTS IdDeCompressor(const BinaryBuffer_Ptr &bw, Id first);

  DARIADB_ST_EXPORTS Id read();

protected:
  Id _prev_value;
};
}
}
