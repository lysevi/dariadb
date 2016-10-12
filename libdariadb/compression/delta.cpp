#include "libdariadb/compression/delta.h"
#include "libdariadb/utils/exception.h"
#include "libdariadb/utils/utils.h"
#include "libdariadb/compression/binarybuffer.h"

#include <cassert>
#include <limits>
#include <sstream>

using namespace dariadb::compression;

const uint16_t delta_64_mask = 0x200;       // 10 0000 0000
const uint16_t delta_64_mask_inv = 0x7F;    // 00 1111 111
const uint16_t delta_256_mask = 0xC00;      // 1100 0000 0000
const uint16_t delta_256_mask_inv = 0x1FF;  // 0001 1111 1111
const uint16_t delta_2047_mask = 0xE000;    // 1110 0000 0000 0000
const uint16_t delta_2047_mask_inv = 0xFFF; // 0000 1111 1111 1111
const uint64_t delta_big_mask =
    0xF00000000; // 1111 [0000 0000] [0000 0000][0000 0000] [0000 0000]
const uint64_t delta_big_mask_inv =
    0xFFFFFFFF; // 0000 1111 1111 1111 1111 1111 1111   1111 1111

DeltaCompressor::DeltaCompressor(const BinaryBuffer_Ptr &bw)
    : BaseCompressor(bw), _is_first(true), _first(0), _prev_delta(0), _prev_time(0) {}

bool DeltaCompressor::append(dariadb::Time t) {
  if (_is_first) {
    _first = t;
    _is_first = false;
    _prev_time = t;
    _prev_delta = 0;
    return true;
  }

  int64_t D = (t - _prev_time) - _prev_delta;
  if (D == 0) {
    if (_bw->free_size() < 1) {
      return false;
    }
    _bw->clrbit().incbit();
  } else {
    if ((-63 < D) && (D < 64)) {
      if (_bw->free_size() < 2) {
        return false;
      }
      auto d = DeltaCompressor::get_delta_64(D);
      _bw->write(d, 9);
    } else {
      if ((-255 < D) && (D < 256)) {
        if (_bw->free_size() < 2) {
          return false;
        }
        auto d = DeltaCompressor::get_delta_256(D);
        _bw->write(d, 11);
      } else {
        if ((-2047 < D) && (D < 2048)) {
          if (_bw->free_size() < 3) {
            return false;
          }
          auto d = DeltaCompressor::get_delta_2048(D);
          _bw->write(d, 15);
        } else {
          if (_bw->free_size() < 6) {
            return false;
          }
          auto d = DeltaCompressor::get_delta_big(D);
          _bw->write(d, 35);
        }
      }
    }
  }

  _prev_delta = D;
  _prev_time = t;
  return true;
}

// DeltaCompressionPosition DeltaCompressor::get_position() const {
//  DeltaCompressionPosition result;
//  result._first = _first;
//  result._is_first = _is_first;
//  result._prev_delta = _prev_delta;
//  result._prev_time = _prev_time;
//  return result;
//}

// void DeltaCompressor::restore_position(const DeltaCompressionPosition &pos) {
//  _first = pos._first;
//  _is_first = pos._is_first;
//  _prev_delta = pos._prev_delta;
//  _prev_time = pos._prev_time;
//}

uint16_t DeltaCompressor::get_delta_64(int64_t D) {
  return delta_64_mask | (delta_64_mask_inv & static_cast<uint16_t>(D));
}

uint16_t DeltaCompressor::get_delta_256(int64_t D) {
  return delta_256_mask | (delta_256_mask_inv & static_cast<uint16_t>(D));
}

uint16_t DeltaCompressor::get_delta_2048(int64_t D) {
  return delta_2047_mask | (delta_2047_mask_inv & static_cast<uint16_t>(D));
}

uint64_t DeltaCompressor::get_delta_big(int64_t D) {
  return delta_big_mask | (delta_big_mask_inv & D);
}

DeltaDeCompressor::DeltaDeCompressor(const BinaryBuffer_Ptr &bw, dariadb::Time first)
    : BaseCompressor(bw), _prev_delta(0), _prev_time(first) {}

dariadb::Time DeltaDeCompressor::read() {
  auto b0 = _bw->getbit();
  _bw->incbit();

  if (b0 == 0) {
    return _prev_time + _prev_delta;
  }

  auto b1 = _bw->getbit();
  _bw->incbit();
  if ((b0 == 1) && (b1 == 0)) { // 64
    int8_t result = static_cast<int8_t>(_bw->read(7));

    if (result > 64) { // is negative
      result = (-128) | result;
    }

    auto ret = _prev_time + result + _prev_delta;
    _prev_delta = result;
    _prev_time = ret;
    return ret;
  }

  auto b2 = _bw->getbit();
  _bw->incbit();
  if ((b0 == 1) && (b1 == 1) && (b2 == 0)) { // 256
    int16_t result = static_cast<int16_t>(_bw->read(8));
    if (result > 256) { // is negative
      result = (-256) | result;
    }
    auto ret = _prev_time + result + _prev_delta;
    _prev_delta = result;
    _prev_time = ret;
    return ret;
  }

  auto b3 = _bw->getbit();
  _bw->incbit();
  if ((b0 == 1) && (b1 == 1) && (b2 == 1) && (b3 == 0)) { // 2048
    int16_t result = static_cast<int16_t>(_bw->read(11));
    if (result > 2048) { // is negative
      result = (-2048) | result;
    }

    auto ret = _prev_time + result + _prev_delta;
    _prev_delta = result;
    _prev_time = ret;
    return ret;
  }

  int64_t result = _bw->read(31);
  if (result > std::numeric_limits<int32_t>::max()) {
    result = (-4294967296) | result;
  }
  auto ret = _prev_time + result + _prev_delta;
  _prev_delta = result;
  _prev_time = ret;
  return ret;
}
