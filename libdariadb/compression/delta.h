#pragma once

#include <libdariadb/compression/base_compressor.h>
#include <libdariadb/st_exports.h>
namespace dariadb {
namespace compression {

class DeltaCompressor : public BaseCompressor {
public:
  DeltaCompressor() = default;
  EXPORT DeltaCompressor(const BinaryBuffer_Ptr &bw);

  EXPORT bool append(Time t);

  EXPORT static uint16_t get_delta_64(int64_t D);
  EXPORT static uint16_t get_delta_256(int64_t D);
  EXPORT static uint16_t get_delta_2048(int64_t D);
  EXPORT static uint64_t get_delta_big(int64_t D);

protected:
  bool _is_first;
  Time _first;
  int64_t _prev_delta;
  Time _prev_time;
};

class DeltaDeCompressor : public BaseCompressor {
public:
  EXPORT DeltaDeCompressor(const BinaryBuffer_Ptr &bw, Time first);

  EXPORT Time read();

protected:
  int64_t _prev_delta;
  Time _prev_time;
};
}
}
