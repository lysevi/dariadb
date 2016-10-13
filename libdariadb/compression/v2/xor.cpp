#include <libdariadb/compression/v2/xor.h>
#include <libdariadb/utils/cz.h>
#include <libdariadb/utils/utils.h>
#include <cassert>
using namespace dariadb;
using namespace dariadb::compression::v2;

const size_t u64_buffer_size = sizeof(uint64_t);

XorCompressor::XorCompressor(const ByteBuffer_Ptr &bw_)
    : BaseCompressor(bw_), _is_first(true), _first(0), _prev_value(0) {}

bool XorCompressor::append(Value v) {
  static_assert(sizeof(Value) == 8, "Value no x64 value");
  auto flat = inner::flat_double_to_int(v);
  if (_is_first) {
	  _first = flat;
	  _is_first = false;
	  _prev_value = flat;
	  return true;
  }
  uint64_t xor_val = _prev_value ^ flat;
  if (xor_val == 0) {
	  if (bw->free_size() < 1) {
		  return false;
	  }
	  bw->write<uint8_t>(0);
	  return true;
  }
  uint8_t flag_byte=0;
  auto lead = dariadb::utils::clz(xor_val);
  auto tail = dariadb::utils::ctz(xor_val);
  const size_t total_bits = sizeof(Value) * 8;
  
  uint8_t count_of_bytes=(total_bits - lead - tail)/8+1;
  flag_byte = count_of_bytes;
  assert(count_of_bytes <= u64_buffer_size);

  if (bw->free_size() < size_t(count_of_bytes + 2)) {
	  return false;
  }
  
  bw->write(flag_byte);
  bw->write(tail);
  
  auto moved_value = xor_val >> tail;
  uint8_t buff[u64_buffer_size];
  *reinterpret_cast<uint64_t*>(buff) = moved_value;
  for (size_t i = 0; i < count_of_bytes; ++i) {
	   bw->write(buff[i]);
  }
 
  _prev_value = flat;
  return true;
}

XorDeCompressor::XorDeCompressor(const ByteBuffer_Ptr &bw_, Value first)
    : BaseCompressor(bw_), _prev_value(inner::flat_double_to_int(first)){}

dariadb::Value XorDeCompressor::read() {
  static_assert(sizeof(dariadb::Value) == 8, "Value no x64 value");
  auto flag_byte = bw->read<uint8_t>();
  if (flag_byte == 0) {//prev==current
	  return inner::flat_int_to_double(_prev_value);
  }
  else {
	  auto byte_count = flag_byte;
	  auto move_count = bw->read<uint8_t>();
	  
	  uint8_t buff[u64_buffer_size];
	  std::fill_n(buff, u64_buffer_size, 0);
	  assert(byte_count <= u64_buffer_size);

	  for (size_t i = 0; i < byte_count; ++i) {
		  buff[i] = bw->read<uint8_t>();
	  }
	  
	  uint64_t raw_value = *reinterpret_cast<uint64_t*>(buff);
	  auto moved = raw_value << move_count;
	  auto ret = moved ^ _prev_value;
	  _prev_value = ret;
	  return inner::flat_int_to_double(ret);
  }
}
