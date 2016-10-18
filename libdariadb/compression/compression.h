#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/compression/binarybuffer.h>
#include <libdariadb/st_exports.h>
#include <memory>

namespace dariadb {
namespace compression {
	//TODO rm pimpl idiom. do like in v2 compression API.
class CopmressedWriter {
public:
  EXPORT CopmressedWriter();
  EXPORT CopmressedWriter(const BinaryBuffer_Ptr &bw_time);
  EXPORT ~CopmressedWriter();
  EXPORT CopmressedWriter(const CopmressedWriter &other);

  EXPORT void swap(CopmressedWriter &other);

  EXPORT CopmressedWriter &operator=(const CopmressedWriter &other);
  EXPORT CopmressedWriter &operator=(CopmressedWriter &&other);

  EXPORT bool append(const Meas &m);
 EXPORT  bool is_full() const;

  EXPORT size_t used_space() const;

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};

class CopmressedReader {
public:
  EXPORT CopmressedReader(const BinaryBuffer_Ptr &bw_time, const Meas &first);
  EXPORT ~CopmressedReader();

  EXPORT Meas read();

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};
}
}
