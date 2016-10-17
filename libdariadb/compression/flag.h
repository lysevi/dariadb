#pragma once

#include <libdariadb/compression/base_compressor.h>
#include <libdariadb/dariadb_st_exports.h>

namespace dariadb {
namespace compression {

class FlagCompressor : public BaseCompressor {
public:
  DARIADB_ST_EXPORTS FlagCompressor(const BinaryBuffer_Ptr &bw);

  DARIADB_ST_EXPORTS bool append(Flag v);

protected:
  bool _is_first;
  Flag _first;
};

class FlagDeCompressor : public BaseCompressor {
public:
  DARIADB_ST_EXPORTS FlagDeCompressor(const BinaryBuffer_Ptr &bw, Flag first);
  DARIADB_ST_EXPORTS Flag read();
protected:
  Flag _prev_value;
};
}
}
