#include "xor.h"
#include "../../utils/cz.h"
#include "../../utils/utils.h"

using namespace dariadb;
using namespace dariadb::compression::v2;

XorCompressor::XorCompressor(const ByteBuffer_Ptr &bw_)
    : BaseCompressor(bw_), _is_first(true), _first(0), _prev_value(0) {}

bool XorCompressor::append(Value v) {
  static_assert(sizeof(Value) == 8, "Value no x64 value");
  return true;
}

XorDeCompressor::XorDeCompressor(const ByteBuffer_Ptr &bw_, Value first)
    : BaseCompressor(bw_), _prev_value(inner::flat_double_to_int(first)),
      _prev_lead(0), _prev_tail(0) {}

dariadb::Value XorDeCompressor::read() {
  static_assert(sizeof(dariadb::Value) == 8, "Value no x64 value");

  return dariadb::Value();
}
