#pragma once

#include "../../utils/exception.h"
#include "../../utils/utils.h"
#include <memory>
#include <ostream>
#include <type_traits>

namespace dariadb {
namespace compression {

class ByteBuffer;
typedef std::shared_ptr<ByteBuffer> ByteBuffer_Ptr;
class ByteBuffer {
public:
  ByteBuffer(const utils::Range &r);
  ~ByteBuffer();

  uint64_t pos() const { return _pos; }

  size_t cap() const { return _cap; }
  size_t free_size() const { return _pos; }
  bool is_full() const { return _pos == 0; }

   template<typename T>
   void write(T v) {
	   static_assert(std::is_pod<T>::value, "write only POD objects.");
	   move_pos(sizeof(T));
	   auto target = reinterpret_cast<T*>(_begin + _pos);
	   *target = v;
   }

   template<typename T>
   T read() {
	   static_assert(std::is_pod<T>::value, "read only POD objects.");
	   auto target = reinterpret_cast<T*>(_begin + _pos);
	   move_pos(sizeof(T));
	   return *target;
   }

  dariadb::utils::Range get_range() const { return dariadb::utils::Range{_begin, _end}; }

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
