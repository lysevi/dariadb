#include <libdariadb/compression/v2/flag.h>

#include <cassert>
#include <limits>
#include <sstream>

using namespace dariadb::compression::v2;

FlagCompressor::FlagCompressor(const ByteBuffer_Ptr &bw_)
    : BaseCompressor(bw_) {
  _is_first = true;
}

bool FlagCompressor::append(dariadb::Flag v) {
  static_assert(sizeof(dariadb::Flag) == 4, "Flag no x32 value");
  if (_is_first) {
    _first = v;
    _is_first = false;
    return true;
  }

  // LEB128
  auto x = v;
  do {
    if (bw->free_size() < 1) {
      return false;
    }
    auto sub_res = x & 0x7fU;
    if (x >>= 7)
      sub_res |= 0x80U;
    bw->write<uint8_t>(static_cast<uint8_t>(sub_res));
  } while (x);

  return true;
}

FlagDeCompressor::FlagDeCompressor(const ByteBuffer_Ptr &bw_,
                                   dariadb::Flag first)
    : BaseCompressor(bw_) {
  _first = first;
  _is_first = true;
  ;
}

dariadb::Flag FlagDeCompressor::read() {
  static_assert(sizeof(dariadb::Flag) == 4, "Flag no x32 value");
  dariadb::Flag result(0);

  size_t bytes = 0;
  while (true) {
    auto readed = bw->read<uint8_t>();
    result |= (readed & 0x7fULL) << (7 * bytes++);
    if (!(readed & 0x80U))
      break;
  }
  return result;
}
