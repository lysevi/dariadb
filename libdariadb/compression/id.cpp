#include "id.h"

#include <cassert>
#include <limits>
#include <sstream>

using namespace dariadb::compression;

IdCompressor::IdCompressor(const BinaryBuffer_Ptr &bw)
    : BaseCompressor(bw), _is_first(true), _first(0) {}

bool IdCompressor::append(dariadb::Id v) {

  static_assert(sizeof(dariadb::Id) == 4, "Id not x32 value");
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
    if (_bw->free_size() < 9) {
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
    _first = v;
  }
  return true;
}

IdDeCompressor::IdDeCompressor(const BinaryBuffer_Ptr &bw, dariadb::Id first)
    : BaseCompressor(bw), _prev_value(first) {}

dariadb::Id IdDeCompressor::read() {
  static_assert(sizeof(dariadb::Id) == 4, "Id not x32 value");
  dariadb::Id result(0);
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
  }
  _prev_value = result;
  return result;
}
