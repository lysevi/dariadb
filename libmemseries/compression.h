#pragma once

#include "exception.h"
#include "meas.h"
#include "binarybuffer.h"
#include <memory>

namespace memseries {
namespace compression {

namespace inner {
union conv {
  double d;
  int64_t i;
};

inline int64_t FlatDouble2Int(double v) {
  conv c;
  c.d = v;
  return c.i;
}
inline double FlatInt2Double(int64_t v) {
  conv c;
  c.i = v;
  return c.d;
}
}

class DeltaCompressor {
public:
  DeltaCompressor() = default;
  DeltaCompressor(const BinaryBuffer &bw);
  DeltaCompressor(const DeltaCompressor &other);
  DeltaCompressor(const DeltaCompressor &&other);
  void swap(DeltaCompressor &other);
  DeltaCompressor& operator=(DeltaCompressor &other);
  DeltaCompressor& operator=(DeltaCompressor &&other);
  ~DeltaCompressor();

  bool append(Time t);

  static uint16_t get_delta_64(int64_t D);
  static uint16_t get_delta_256(int64_t D);
  static uint16_t get_delta_2048(int64_t D);
  static uint64_t get_delta_big(int64_t D);

  bool is_full() const { return _bw.is_full(); }

protected:
  bool _is_first;
  BinaryBuffer _bw;
  Time _first;
  uint64_t _prev_delta;
  Time _prev_time;
};

class DeltaDeCompressor {
public:
  DeltaDeCompressor() = default;
  DeltaDeCompressor(const BinaryBuffer &bw, Time first);
  ~DeltaDeCompressor();
  DeltaDeCompressor(const DeltaDeCompressor &other);
  void swap(DeltaDeCompressor &other);
  DeltaDeCompressor& operator=(DeltaDeCompressor &other);
  DeltaDeCompressor& operator=(DeltaDeCompressor &&other);

  Time read();

  bool is_full() const { return _bw.is_full(); }

protected:
  BinaryBuffer _bw;
  int64_t _prev_delta;
  Time _prev_time;
};

class XorCompressor {
public:
  XorCompressor() = default;
  XorCompressor(const BinaryBuffer &bw);
  ~XorCompressor();
  XorCompressor(const XorCompressor &other);
  void swap(XorCompressor &other);
  XorCompressor& operator=(XorCompressor &other);
  XorCompressor& operator=(XorCompressor &&other);

  bool append(Value v);
  static uint8_t zeros_lead(uint64_t v);
  static uint8_t zeros_tail(uint64_t v);

  bool is_full() const { return _bw.is_full(); }

protected:
  bool _is_first;
  BinaryBuffer _bw;
  uint64_t _first;
  uint64_t _prev_value;
  uint8_t _prev_lead;
  uint8_t _prev_tail;
};

class XorDeCompressor {
public:
  XorDeCompressor() = default;
  XorDeCompressor(const BinaryBuffer &bw, Value first);
  ~XorDeCompressor() = default;
  XorDeCompressor(const XorDeCompressor &other);
  void swap(XorDeCompressor &other);
  XorDeCompressor& operator=(XorDeCompressor &other);
  XorDeCompressor& operator=(XorDeCompressor &&other);

  Value read();

  bool is_full() const { return _bw.is_full(); }

protected:
  BinaryBuffer _bw;
  uint64_t _prev_value;
  uint8_t _prev_lead;
  uint8_t _prev_tail;
};

class FlagCompressor {
public:
  FlagCompressor() = default;
  FlagCompressor(const BinaryBuffer &bw);
  ~FlagCompressor();
  FlagCompressor(const FlagCompressor &other);
  void swap(FlagCompressor &other);
  FlagCompressor& operator=(FlagCompressor &other);
  FlagCompressor& operator=(FlagCompressor &&other);

  bool append(Flag v);

  bool is_full() const { return _bw.is_full(); }

protected:
  BinaryBuffer _bw;
  bool _is_first;
  Flag _first;
};

class FlagDeCompressor {
public:
  FlagDeCompressor() = default;
  FlagDeCompressor(const BinaryBuffer &bw, Flag first);
  ~FlagDeCompressor() = default;
  FlagDeCompressor(const FlagDeCompressor &other);
  void swap(FlagDeCompressor &other);
  FlagDeCompressor& operator=(FlagDeCompressor &other);

  Flag read();

  bool is_full() const { return _bw.is_full(); }

protected:
  BinaryBuffer _bw;
  Flag _prev_value;
};

class CopmressedWriter {
public:
  CopmressedWriter() = default;
  CopmressedWriter(BinaryBuffer bw_time, BinaryBuffer bw_values,
                   BinaryBuffer bw_flags);
  ~CopmressedWriter();
  CopmressedWriter(const CopmressedWriter &other);
  void swap(CopmressedWriter &other);
  CopmressedWriter& operator=(CopmressedWriter &other);
  CopmressedWriter& operator=(CopmressedWriter &&other);


  bool append(const Meas &m);

  bool is_full() const { return _is_full; }

protected:
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
  CopmressedReader(BinaryBuffer bw_time, BinaryBuffer bw_values,
                   BinaryBuffer bw_flags, Meas first);
  ~CopmressedReader();

  Meas read();
  bool is_full() const {
    return this->time_dcomp.is_full() || this->value_dcomp.is_full() ||
           this->flag_dcomp.is_full();
  }

protected:
  Meas _first;

  DeltaDeCompressor time_dcomp;
  XorDeCompressor value_dcomp;
  FlagDeCompressor flag_dcomp;
};
}
}
