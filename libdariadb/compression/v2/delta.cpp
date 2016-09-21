#include "delta.h"
#include <cassert>
#include <limits>
#include <sstream>

using namespace dariadb::compression::v2;

DeltaCompressor::DeltaCompressor(const ByteBuffer_Ptr &bw)
    : BaseCompressor(bw), _is_first(true), _first(0), _prev_delta(0), _prev_time(0) {}

bool DeltaCompressor::append(dariadb::Time t) {
  return true;
}

DeltaDeCompressor::DeltaDeCompressor(const ByteBuffer_Ptr &bw, dariadb::Time first)
    : BaseCompressor(bw), _prev_delta(0), _prev_time(first) {}

dariadb::Time DeltaDeCompressor::read() {
  return Time(0);
}
