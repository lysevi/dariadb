#include "xor.h"
#include "../utils/cz.h"
#include "../utils/utils.h"
#include "binarybuffer.h"
#include <cassert>
#include <limits>
#include <sstream>

using namespace dariadb;
using namespace dariadb::compression;

XorCompressor::XorCompressor(const BinaryBuffer_Ptr &bw)
    : BaseCompressor(bw), _is_first(true), _first(0), _prev_value(0) {}

bool XorCompressor::append(Value v) {
  static_assert(sizeof(Value) == 8, "Value no x64 value");
  auto flat = inner::flat_double_to_int(v);
  if (_is_first) {
    _first = flat;
    _is_first = false;
    _prev_value = flat;
    _prev_lead = 64;
    _prev_tail = 64;
    return true;
  }
  if (_bw->free_size() < 9) {
    return false;
  }
  auto xor_val = _prev_value ^ flat;
  if (xor_val == 0) {
    if (_bw->free_size() < 1) {
      return false;
    }
    _bw->clrbit().incbit();
    return true;
  }

  _bw->setbit().incbit();

  auto lead = dariadb::utils::clz(xor_val);
  auto tail = dariadb::utils::ctz(xor_val);

  if ((_prev_lead == lead) && (_prev_tail == tail)) {
    _bw->clrbit().incbit();
  } else {

    if (_bw->free_size() < 16) {
      return false;
    }
    _bw->setbit().incbit();
    _bw->write((uint16_t)lead, int8_t(5));
    _bw->write((uint16_t)tail, int8_t(5));
  }
  int8_t bits_to_write = (64 - lead - tail);
  if (_bw->free_size() < size_t(bits_to_write / 8 + 1)) {
    return false;
  }
  assert(bits_to_write != 0);

  xor_val = xor_val >> tail;
  _bw->write(xor_val, bits_to_write);

  _prev_value = flat;
  _prev_lead = lead;
  _prev_tail = tail;
  return true;
}

XorCompressionPosition XorCompressor::get_position() const {
  XorCompressionPosition result;
  result._first = _first;
  result._is_first = _is_first;
  result._prev_lead = _prev_lead;
  result._prev_tail = _prev_tail;
  result._prev_value = _prev_value;
  return result;
}

void XorCompressor::restore_position(const XorCompressionPosition &pos) {
  _first = pos._first;
  _is_first = pos._is_first;
  _prev_lead = pos._prev_lead;
  _prev_tail = pos._prev_tail;
  _prev_value = pos._prev_value;
}

XorDeCompressor::XorDeCompressor(const BinaryBuffer_Ptr &bw, Value first)
    : BaseCompressor(bw), _prev_value(inner::flat_double_to_int(first)), _prev_lead(0),
      _prev_tail(0) {}

dariadb::Value XorDeCompressor::read() {
  static_assert(sizeof(dariadb::Value) == 8, "Value no x64 value");
  auto b0 = _bw->getbit();
  _bw->incbit();
  if (b0 == 0) {
    return inner::flat_int_to_double(_prev_value);
  }

  auto b1 = _bw->getbit();
  _bw->incbit();

  if (b1 == 1) {
    uint8_t leading = static_cast<uint8_t>(_bw->read(5));

    uint8_t tail = static_cast<uint8_t>(_bw->read(5));
    uint64_t result = 0;
    int8_t bit2read = 64 - leading - tail;
    assert(bit2read != 0);

    result = _bw->read(bit2read);
    result = result << tail;

    _prev_lead = leading;
    _prev_tail = tail;
    auto ret = result ^ _prev_value;
    _prev_value = ret;
    return inner::flat_int_to_double(ret);
  } else {
    uint64_t result = 0;
    int8_t but2read = 64 - _prev_lead - _prev_tail;
    assert(but2read != 0);
    result = _bw->read(but2read);
    result = result << _prev_tail;

    auto ret = result ^ _prev_value;
    _prev_value = ret;
    return inner::flat_int_to_double(ret);
  }
}
