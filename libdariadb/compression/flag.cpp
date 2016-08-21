#include "flag.h"

#include <cassert>
#include <limits>
#include <sstream>

using namespace dariadb::compression;

FlagCompressor::FlagCompressor(const BinaryBuffer_Ptr &bw)
    : BaseCompressor(bw), _is_first(true), _first(0) {}

bool FlagCompressor::append(dariadb::Flag v) {

  static_assert(sizeof(dariadb::Flag) == 4, "Flag no x32 value");
  if (_is_first) {
    this->_first = v;
    this->_is_first = false;
    return true;
  }

  if (v == _first) {
    if (_bw->free_size() < 1) {
      return false;
    }
    _bw->clrbit().incbit();
  } else {
    if (_bw->free_size() < 5) {
      return false;
    }
    _bw->setbit().incbit();
    // LEB128
    auto x = v;
    do {
      auto sub_res = x & 0x7fU;
      if (x >>= 7)
        sub_res |= 0x80U;
      _bw->write(uint16_t(sub_res), 8);
    } while (x);
    //_bw->write(uint64_t(v), 31);
    _first = v;
  }
  return true;
}

// FlagCompressionPosition FlagCompressor::get_position() const {
//  FlagCompressionPosition result;
//  result._first = _first;
//  result._is_first = _is_first;
//  return result;
//}

// void FlagCompressor::restore_position(const FlagCompressionPosition &pos) {
//  _first = pos._first;
//  _is_first = pos._is_first;
//}

FlagDeCompressor::FlagDeCompressor(const BinaryBuffer_Ptr &bw, dariadb::Flag first)
    : BaseCompressor(bw), _prev_value(first) {}

dariadb::Flag FlagDeCompressor::read() {
  static_assert(sizeof(dariadb::Flag) == 4, "Flag no x32 value");
  dariadb::Flag result(0);
  if (_bw->getbit() == 0) {
    _bw->incbit();
    result = _prev_value;
  } else {
    _bw->incbit();
    size_t bytes = 0;
    while (true) {
      auto readed = _bw->read(8);
      result |= (readed & 0x7fULL) << (7 * bytes++);
      if (!(readed & 0x80U))
        break;
    }
    // result = (dariadb::Flag)_bw->read(31);
  }
  _prev_value = result;
  return result;
}
