#include <libdariadb/compression/delta.h>
#include <libdariadb/utils/utils.h>
#include <libdariadb/utils/bitoperations.h>
#include <limits>

using namespace dariadb::compression;
#pragma pack(push, 1)
union conv_32 {
  uint32_t big;
  struct {
    uint16_t lo;
    uint8_t hi;
  } small;
};
#pragma pack(pop)

const uint8_t delta_1b_mask = 0x80;     // 1000 0000
const uint8_t delta_1b_mask_inv = 0x3F; // 0011 1111

const uint16_t delta_2b_mask = 0xC000;     // 1100 0000 0000 0000
const uint16_t delta_2b_mask_inv = 0x1FFF; // 0001 1111 1111 1111

const uint32_t delta_3b_mask = 0xE00000;    // 1110 0000 0000 0000 0000 0000
const uint32_t delta_3b_mask_inv = 0xFFFFF; // 0000 1111 1111 1111 1111 1111

uint8_t get_delta_1b(int64_t D) {
  return delta_1b_mask | (delta_1b_mask_inv & static_cast<uint8_t>(D));
}

uint16_t get_delta_2b(int64_t D) {
  return delta_2b_mask | (delta_2b_mask_inv & static_cast<uint16_t>(D));
}

uint32_t get_delta_3b(int64_t D) {
  return delta_3b_mask | (delta_3b_mask_inv & static_cast<uint32_t>(D));
}

DeltaCompressor::DeltaCompressor(const ByteBuffer_Ptr &bw_)
    : BaseCompressor(bw_), is_first(true), first(0), prev_delta(0),
      prev_time(0) {}

bool DeltaCompressor::append(dariadb::Time t) {
  if (is_first) {
    first = t;
    is_first = false;
    prev_time = t;
    prev_delta = 0;
    return true;
  }

  int64_t D = (t - prev_time) - prev_delta;

  if ((-32 < D) && (D < 32)) {
    if (bw->free_size() < 1) {
      return false;
    }
    auto d = get_delta_1b(D);
    bw->write<uint8_t>(d);
  } else {
    if ((-4096 < D) && (D < 4096)) {
      if (bw->free_size() < 2) {
        return false;
      }
      auto d = get_delta_2b(D);
      bw->write<uint16_t>(d);
    } else {
      if ((-524288 < D) && (D < 524287)) {
        if (bw->free_size() < 3) {
          return false;
        }
        auto d = get_delta_3b(D);
        conv_32 c;
        c.big = d;

        bw->write<uint8_t>(c.small.hi);
        bw->write<uint16_t>(c.small.lo);
      } else {
        if (bw->free_size() < 9) {
          return false;
        }
        bw->write<uint8_t>(uint8_t(0));
        bw->write<uint64_t>(D);
      }
    }
  }

  prev_delta = D;
  prev_time = t;
  return true;
}

DeltaDeCompressor::DeltaDeCompressor(const ByteBuffer_Ptr &bw_,
                                     dariadb::Time first)
    : BaseCompressor(bw_), prev_delta(0), prev_time(first) {}

dariadb::Time DeltaDeCompressor::read() {
  auto first_byte = bw->read<uint8_t>();

  int64_t result = 0;
  if ((first_byte & delta_1b_mask) == delta_1b_mask &&
      !utils::BitOperations::check(first_byte, 6)) {
    result = first_byte & delta_1b_mask_inv;
    if (result > 32) { // negative
      result = (-64) | result;
    }
  } else {
    if ((first_byte & (delta_2b_mask >> 8)) == (delta_2b_mask >> 8) &&
        !utils::BitOperations::check(first_byte, 5)) {
      auto second = bw->read<uint8_t>();
      auto first_unmasked = first_byte & (delta_2b_mask_inv >> 8);
      result = ((uint16_t)first_unmasked << 8) | (uint16_t)second;

      if (result > 4096) { // negative
        result = (-4096) | result;
      }
    } else {
      if ((first_byte & (delta_3b_mask >> (16))) == (delta_3b_mask >> 16)) {
        auto second = bw->read<uint16_t>();
        auto first_unmasked = first_byte & (uint8_t)(delta_3b_mask_inv >> (16));
        conv_32 c;
        c.big = 0;
        c.small.hi = (uint8_t)first_unmasked;
        c.small.lo = second;
        result = c.big;
        if (result > 524287) { // negative
          result = (-524287) | result;
        }
      } else {
        if (first_byte == 0) {
          result = bw->read<uint64_t>();
        } else {
          ENSURE(false);
        }
      }
    }
  }
  auto ret = prev_time + result + prev_delta;
  prev_delta = result;
  prev_time = ret;
  return ret;
}
