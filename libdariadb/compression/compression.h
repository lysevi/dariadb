#pragma once

#include <memory>

#include <libdariadb/compression/bytebuffer.h>
#include <libdariadb/compression/delta.h>
#include <libdariadb/compression/flag.h>
#include <libdariadb/compression/xor.h>
#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>

namespace dariadb {
namespace compression {
class CopmressedWriter {
public:
  EXPORT CopmressedWriter(const ByteBuffer_Ptr &bw_time);
  EXPORT ~CopmressedWriter();

  EXPORT bool append(const Meas &m);
  bool isFull() const { return _is_full; }

  size_t usedSpace() const { return time_comp.used_space(); }

  ByteBuffer_Ptr getBinaryBuffer() const { return _bb; }

protected:
  ByteBuffer_Ptr _bb;
  Meas _first;
  bool _is_first;
  bool _is_full;
  DeltaCompressor time_comp;
  XorCompressor value_comp;
  FlagCompressor flag_comp;
};

class CopmressedReader {
public:
  CopmressedReader() = default;
  EXPORT CopmressedReader(const ByteBuffer_Ptr &bw_time, const Meas &first);
  EXPORT ~CopmressedReader();

  dariadb::Meas read() {
    Meas result{};
    result.id = _first.id;
    result.time = time_dcomp.read();
    result.value = value_dcomp.read();
    result.flag = flag_dcomp.read();
    return result;
  }

protected:
  dariadb::Meas _first;
  DeltaDeCompressor time_dcomp;
  XorDeCompressor value_dcomp;
  FlagDeCompressor flag_dcomp;
};
}
}
