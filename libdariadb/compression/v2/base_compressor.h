#pragma once
#include "bytebuffer.h"
#include "../../meas.h"

namespace dariadb {
namespace compression {
namespace v2 {

struct BaseCompressor {
  BaseCompressor(const ByteBuffer_Ptr &bw);
  bool is_full() const { return _bw->is_full(); }
  size_t used_space() const { return _bw->cap() - _bw->pos(); }

  ByteBuffer_Ptr _bw;
};

}
}
}
