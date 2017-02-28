#pragma once

#include <libdariadb/st_exports.h>
#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/utils.h>
#include <memory>
#include <ostream>
#include <type_traits>

namespace dariadb {
namespace compression {

struct Range {
  uint8_t *begin;
  uint8_t *end;
  Range() { begin = end = nullptr; }

  Range(uint8_t *_begin, uint8_t *_end) {
    begin = _begin;
    end = _end;
  }
};

class ByteBuffer;
typedef std::shared_ptr<ByteBuffer> ByteBuffer_Ptr;
class ByteBuffer {
public:
  EXPORT ByteBuffer(const Range &r);
  EXPORT ~ByteBuffer();

  uint32_t pos() const { return _pos; }
  void set_pos(uint32_t bw_pos) { _pos = bw_pos; }
  size_t cap() const { return _cap; }
  size_t free_size() const { return _pos; }
  bool is_full() const { return _pos == 0; }
  void reset_pos() { _pos = _cap - 1; }
  /// return offset of value sizeof(t) relative of current pos.
  template <typename T> T *offset_off() {
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

  Range get_range() const { return Range{_begin, _end}; }

protected:
  inline void move_pos(int8_t count) {
    _pos -= count;
#ifdef DEBUG
    if (_pos > _cap) {
      throw MAKE_EXCEPTION("BinaryBuffer::move_pos");
    }
#endif
  }

protected:
  uint8_t *_begin, *_end;
  uint32_t _cap;
  uint32_t _pos;
};
}
}
