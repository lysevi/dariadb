#pragma once

#include "../meas.h"
#include "binarybuffer.h"
#include <memory>

namespace dariadb {
namespace compression {
	//TODO rm pimpl idiom. do like in v2 compression API.
class CopmressedWriter {
public:
  CopmressedWriter();
  CopmressedWriter(const BinaryBuffer_Ptr &bw_time);
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
  CopmressedReader(const BinaryBuffer_Ptr &bw_time, const Meas &first);
  ~CopmressedReader();

  Meas read();

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};
}
}
