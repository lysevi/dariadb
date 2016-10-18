#pragma once
#include <libdariadb/compression/v2/bytebuffer.h>
#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
namespace dariadb {
namespace compression {
namespace v2 {

struct BaseCompressor {
  EXPORT BaseCompressor(const ByteBuffer_Ptr &bw_);
  bool is_full() const { return bw->is_full(); }
   size_t used_space() const { return bw->cap() - bw->pos(); }

  ByteBuffer_Ptr bw;
};

}
}
}
