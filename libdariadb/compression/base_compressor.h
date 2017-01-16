#pragma once
#include <libdariadb/compression/bytebuffer.h>
#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
namespace dariadb {
namespace compression {

struct BaseCompressor {
  EXPORT BaseCompressor(const ByteBuffer_Ptr &bw_);
  bool is_full() const { return bw->is_full(); }
  size_t used_space() const { return bw->cap() - bw->pos(); }

  ByteBuffer_Ptr bw;
};
}
}
