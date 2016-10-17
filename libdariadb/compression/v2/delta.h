#pragma once

#include <libdariadb/compression/v2/base_compressor.h>
#include <libdariadb/dariadb_st_exports.h>

namespace dariadb {
namespace compression {
namespace v2 {

struct DeltaCompressor : public BaseCompressor {
  DARIADB_ST_EXPORTS DeltaCompressor(const ByteBuffer_Ptr &bw);

  DARIADB_ST_EXPORTS bool append(Time t);

  bool is_first;
  Time first;
  int64_t prev_delta;
  Time prev_time;
};

struct DeltaDeCompressor : public BaseCompressor {
  DARIADB_ST_EXPORTS DeltaDeCompressor(const ByteBuffer_Ptr &bw, Time first);

  DARIADB_ST_EXPORTS Time read();

  int64_t prev_delta;
  Time prev_time;
};
}
}
}
