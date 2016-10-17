#pragma once

#include <libdariadb/compression/v2/base_compressor.h>
#include <libdariadb/dariadb_st_exports.h>

namespace dariadb {
namespace compression {
namespace v2 {
struct FlagCompressor : public BaseCompressor {
  DARIADB_ST_EXPORTS FlagCompressor(const ByteBuffer_Ptr &bw);

  DARIADB_ST_EXPORTS bool append(Flag v);
  Flag _first;
  bool _is_first;
};

struct FlagDeCompressor : public BaseCompressor {
  DARIADB_ST_EXPORTS FlagDeCompressor(const ByteBuffer_Ptr &bw, Flag first);

  DARIADB_ST_EXPORTS Flag read();
  Flag _first;
  bool _is_first;
};
}
}
}
