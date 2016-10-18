#pragma once

#include <libdariadb/compression/v2/base_compressor.h>
#include <libdariadb/st_exports.h>

namespace dariadb {
namespace compression {
namespace v2 {
struct FlagCompressor : public BaseCompressor {
  EXPORT FlagCompressor(const ByteBuffer_Ptr &bw);

  EXPORT bool append(Flag v);
  Flag _first;
  bool _is_first;
};

struct FlagDeCompressor : public BaseCompressor {
  EXPORT FlagDeCompressor(const ByteBuffer_Ptr &bw, Flag first);

  EXPORT Flag read();
  Flag _first;
  bool _is_first;
};
}
}
}
