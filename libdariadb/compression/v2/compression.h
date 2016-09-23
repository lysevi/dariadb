#pragma once

#include "../../meas.h"
#include "bytebuffer.h"
#include <memory>

namespace dariadb {
namespace compression {
namespace v2 {
class CopmressedWriter {
public:
  CopmressedWriter();
  CopmressedWriter(const ByteBuffer_Ptr &bw_time);
  ~CopmressedWriter();
  CopmressedWriter(const CopmressedWriter &other);

  void swap(CopmressedWriter &other);

  CopmressedWriter &operator=(const CopmressedWriter &other);
  CopmressedWriter &operator=(CopmressedWriter &&other);

  bool append(const Meas &m);
  bool is_full() const;

  size_t used_space() const;

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};

class CopmressedReader {
public:
  CopmressedReader(const ByteBuffer_Ptr &bw_time, const Meas &first);
  ~CopmressedReader();

  Meas read();

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};
}
}
}
