#include "id.h"

#include <cassert>
#include <limits>
#include <sstream>

using namespace dariadb::compression;

IdCompressor::IdCompressor(const BinaryBuffer_Ptr &bw)
    : BaseCompressor(bw), _is_first(true), _first(0) {}

bool IdCompressor::append(dariadb::Id v) {

  static_assert(sizeof(dariadb::Id) == 8, "Id no x64 value");
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
    _bw->write(uint64_t(v), 64);

    _first = v;
  }
  return true;
}

IdCompressionPosition IdCompressor::get_position() const {
  IdCompressionPosition result;
  result._first = _first;
  result._is_first = _is_first;
  return result;
}

void IdCompressor::restore_position(const IdCompressionPosition &pos) {
  _first = pos._first;
  _is_first = pos._is_first;
}

IdDeCompressor::IdDeCompressor(const BinaryBuffer_Ptr &bw, dariadb::Id first)
    : BaseCompressor(bw), _prev_value(first) {}

dariadb::Id IdDeCompressor::read() {
  static_assert(sizeof(dariadb::Id) == 8, "Id no x64 value");
  dariadb::Id result(0);
  if (_bw->getbit() == 0) {
    _bw->incbit();
    result = _prev_value;
  } else {
    _bw->incbit();
    result = (dariadb::Id)_bw->read(64);
  }
  _prev_value = result;
  return result;
}
