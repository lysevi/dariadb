#pragma once

#include "base_compressor.h"
#include "positions.h"

namespace dariadb {
namespace compression {
namespace inner {
//            union conv{
//                dariadb::Value v;
//                int64_t i;
//            };
inline int64_t flat_double_to_int(dariadb::Value v) {
  static_assert(sizeof(dariadb::Value) == sizeof(int64_t), "size not equal");
  //                conv c;
  //                c.v=v;
  //                return c.i;
  auto result = reinterpret_cast<int64_t *>(&v);
  return *result;
}
inline dariadb::Value flat_int_to_double(int64_t i) {
  //                conv c;
  //                c.i=i;
  //                return c.v;
  auto result = reinterpret_cast<dariadb::Value *>(&i);
  return *result;
}
}

class XorCompressor : public BaseCompressor {
public:
  XorCompressor(const BinaryBuffer_Ptr &bw);

  bool append(Value v);
  XorCompressionPosition get_position() const;
  void restore_position(const XorCompressionPosition &pos);

protected:
  bool _is_first;
  uint64_t _first;
  uint64_t _prev_value;
  uint8_t _prev_lead;
  uint8_t _prev_tail;
};

class XorDeCompressor : public BaseCompressor {
public:
  XorDeCompressor(const BinaryBuffer_Ptr &bw, Value first);

  Value read();

protected:
  uint64_t _prev_value;
  uint8_t _prev_lead;
  uint8_t _prev_tail;
};
}
}
