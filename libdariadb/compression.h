#pragma once

#include "compression/binarybuffer.h"
#include "compression/positions.h"
#include "meas.h"
#include <memory>

namespace dariadb {
namespace compression {

class CopmressedWriter {
public:
  struct Position {
    IdCompressionPosition id_pos;
    DeltaCompressionPosition time_pos;
    XorCompressionPosition value_pos;
    FlagCompressionPosition flag_pos, src_pos;
    Meas first;
    bool is_first;
    bool is_full;
  };
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

  Position get_position() const;
  void restore_position(const Position &pos);

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};

class CopmressedReader {
public:
  CopmressedReader();
  CopmressedReader(const BinaryBuffer_Ptr &bw_time, const Meas &first);
  ~CopmressedReader();
  CopmressedReader(const CopmressedReader &other);
  void swap(CopmressedReader &other);
  CopmressedReader &operator=(const CopmressedReader &other);
  CopmressedReader &operator=(CopmressedReader &&other);

  Meas read();
protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};
}
}
