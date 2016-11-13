#pragma once

#include <libdariadb/compression/base_compressor.h>
#include <libdariadb/st_exports.h>

namespace dariadb {
namespace compression {
namespace inner {

inline int64_t flat_double_to_int(dariadb::Value v) {
  static_assert(sizeof(dariadb::Value) == sizeof(int64_t), "size not equal");
  auto result = reinterpret_cast<int64_t *>(&v);
  return *result;
}
inline dariadb::Value flat_int_to_double(int64_t i) {
  auto result = reinterpret_cast<dariadb::Value *>(&i);
  return *result;
}
}

struct XorCompressor : public BaseCompressor {
public:
  EXPORT XorCompressor(const ByteBuffer_Ptr &bw_);

  EXPORT bool append(Value v);

  bool _is_first;
  uint64_t _first;
  uint64_t _prev_value;
};

struct XorDeCompressor : public BaseCompressor {
public:
  EXPORT XorDeCompressor(const ByteBuffer_Ptr &bw, Value first);

  EXPORT Value read();

  uint64_t _prev_value;
};

}
}
