#pragma once

#include <memory>

#include "libdariadb/meas.h"
#include "libdariadb/compression/v2/bytebuffer.h"
#include "libdariadb/compression/v2/delta.h"
#include "libdariadb/compression/v2/flag.h"
#include "libdariadb/compression/v2/xor.h"

namespace dariadb {
namespace compression {
namespace v2 {
class CopmressedWriter {
public:
  CopmressedWriter(const ByteBuffer_Ptr &bw_time);
  ~CopmressedWriter();

  bool append(const Meas &m);
  bool is_full() const { return _is_full; }

  size_t used_space() const { return time_comp.used_space(); }

  ByteBuffer_Ptr get_bb()const { return _bb; }
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
  CopmressedReader(const ByteBuffer_Ptr &bw_time, const Meas &first);
  ~CopmressedReader();


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
}
