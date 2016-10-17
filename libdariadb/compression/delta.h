#pragma once

#include <libdariadb/compression/base_compressor.h>
#include <libdariadb/dariadb_st_exports.h>
namespace dariadb {
namespace compression {

class DeltaCompressor : public BaseCompressor {
public:
  DeltaCompressor() = default;
  DARIADB_ST_EXPORTS DeltaCompressor(const BinaryBuffer_Ptr &bw);

  DARIADB_ST_EXPORTS bool append(Time t);

  DARIADB_ST_EXPORTS static uint16_t get_delta_64(int64_t D);
  DARIADB_ST_EXPORTS static uint16_t get_delta_256(int64_t D);
  DARIADB_ST_EXPORTS static uint16_t get_delta_2048(int64_t D);
  DARIADB_ST_EXPORTS static uint64_t get_delta_big(int64_t D);

protected:
  bool _is_first;
  Time _first;
  int64_t _prev_delta;
  Time _prev_time;
};

class DeltaDeCompressor : public BaseCompressor {
public:
  DARIADB_ST_EXPORTS DeltaDeCompressor(const BinaryBuffer_Ptr &bw, Time first);

  DARIADB_ST_EXPORTS Time read();

protected:
  int64_t _prev_delta;
  Time _prev_time;
};
}
}
