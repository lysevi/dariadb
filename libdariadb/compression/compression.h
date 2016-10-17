#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/compression/binarybuffer.h>
#include <libdariadb/dariadb_st_exports.h>
#include <memory>

namespace dariadb {
namespace compression {
	//TODO rm pimpl idiom. do like in v2 compression API.
class CopmressedWriter {
public:
  DARIADB_ST_EXPORTS CopmressedWriter();
  DARIADB_ST_EXPORTS CopmressedWriter(const BinaryBuffer_Ptr &bw_time);
  DARIADB_ST_EXPORTS ~CopmressedWriter();
  DARIADB_ST_EXPORTS CopmressedWriter(const CopmressedWriter &other);

  DARIADB_ST_EXPORTS void swap(CopmressedWriter &other);

  DARIADB_ST_EXPORTS CopmressedWriter &operator=(const CopmressedWriter &other);
  DARIADB_ST_EXPORTS CopmressedWriter &operator=(CopmressedWriter &&other);

  DARIADB_ST_EXPORTS bool append(const Meas &m);
 DARIADB_ST_EXPORTS  bool is_full() const;

  DARIADB_ST_EXPORTS size_t used_space() const;

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};

class CopmressedReader {
public:
  DARIADB_ST_EXPORTS CopmressedReader(const BinaryBuffer_Ptr &bw_time, const Meas &first);
  DARIADB_ST_EXPORTS ~CopmressedReader();

  DARIADB_ST_EXPORTS Meas read();

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};
}
}
