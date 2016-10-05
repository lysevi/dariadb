#pragma once

#include "../utils/locker.h"
#include <atomic>
#include <cassert>
#include <cstdint>
#include <type_traits>

namespace dariadb {
namespace utils {

template <typename T> class LockFreeArray {
  static_assert(std::is_pod<T>::value || std::is_copy_assignable<T>::value ||
                    std::is_default_constructible<T>::value,
                "Value must be POD, copy-assign and default ctor implemented.");

public:
  LockFreeArray() {
    _pos = 0;
    _array = nullptr;
    _size = 0;
  }

  LockFreeArray(const size_t size) : _size(size) {
    _pos = 0;
    _array = new std::atomic<T>[_size];
	std::fill(_array, _array + _size, T());
  }

  LockFreeArray(const LockFreeArray &other) = delete;
  LockFreeArray(const LockFreeArray &&other)
      : _size(other._size), _pos(other._pos), _array(other._array) {
    other._size = 0;
    other._array = nullptr;
  }
  ~LockFreeArray() {
    if (_array != nullptr) {
      delete[] _array;
    }
  }

  size_t cap() const { return _size - _pos; }
  size_t size() const { return _size; }

  bool try_store(size_t index, const T &t) {
    assert(index < _size);
    auto old = _array[index].load();
    return _array[index].compare_exchange_weak(old, t);
  }

protected:
  size_t _size;
  std::atomic<T> *_array;
  size_t _pos;
};
}
}