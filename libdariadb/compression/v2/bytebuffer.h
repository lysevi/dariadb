#pragma once

#include "../../utils/exception.h"
#include "../../utils/utils.h"
#include <memory>
#include <ostream>
#include <type_traits>

namespace dariadb {
namespace compression {
namespace v2 {
class ByteBuffer;
typedef std::shared_ptr<ByteBuffer> ByteBuffer_Ptr;
class ByteBuffer {
public:
  ByteBuffer(const utils::Range &r);
  ~ByteBuffer();

  uint64_t pos() const { return _pos; }
  void set_pos(uint64_t bw_pos) { _pos = bw_pos; }
  size_t cap() const { return _cap; }
  size_t free_size() const { return _pos; }
  bool is_full() const { return _pos == 0; }
  void reset_pos(){ _pos = _cap - 1; }
  ///return offset of value sizeof(t) relative of current pos.
  template<typename T>
  T* offset_off() {
	  static_assert(std::is_pod<T>::value, "only POD objects.");
	  move_pos(sizeof(T));
	  auto target = reinterpret_cast<T *>(_begin + _pos);
	  return target;
  }

  template <typename T> void write(T v) {
	auto target = offset_off<T>();
    *target = v;
  }

  template <typename T> T read() {
	auto target = offset_off<T>();
    return *target;
  }

  dariadb::utils::Range get_range() const { return dariadb::utils::Range{ _begin, _end }; }
protected:
  inline void move_pos(int8_t count) {
    _pos -= count;
    if (_pos > _cap) {
      throw MAKE_EXCEPTION("BinaryBuffer::move_pos");
    }
  }

protected:
  uint8_t *_begin, *_end;
  uint64_t _cap;
  uint64_t _pos;
};
}
}
}
