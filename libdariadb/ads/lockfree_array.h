#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <type_traits>

namespace dariadb {
namespace ads {

template <typename T> class LockFreeArray {
public:
  LockFreeArray() {
    _pos.store(0);
    _array = nullptr;
    _size = 0;
  }

  LockFreeArray(const size_t size) : _size(size) {
    _pos.store(0);
    _array = new std::atomic<T>[_size];
    std::fill(_array, _array + _size, T());
  }

  LockFreeArray(LockFreeArray &&other)
      : _size(other._size), _pos(other._pos.load()), _array(other._array) {
    other._size = 0;
    other._array = nullptr;
  }

  LockFreeArray(const LockFreeArray &other) {
    _size = other._size;
    _pos.store(other._pos);
    _array = new std::atomic<T>[_size];
    for (size_t i = 0; i < _size; ++i) {
      _array[i].store(other._array[i].load());
    }
  }
  LockFreeArray &operator=(const LockFreeArray &other) {
    if (this == &other) {
      return *this;
    }
    _size = other._size;
    _pos.store(other._pos);
    _array = new std::atomic<T>[_size];
    for (size_t i = 0; i < _size; ++i) {
      _array[i].store(other._array[i].load());
    }
    return *this;
  }
  LockFreeArray &operator=(LockFreeArray &&other) {
    _size = other._size;
    _pos.store(other._pos.load());
    _array = other._array;
    other._size = 0;
    other._array = nullptr;
  }
  ~LockFreeArray() {
    if (_array != nullptr) {
      delete[] _array;
    }
  }

  size_t size() const { return _size; }

  bool compare_exchange(size_t index, T &old, const T &t) {
    assert(index < _size);
    return _array[index].compare_exchange_weak(old, t);
  }

  void store(size_t index, const T &t) {
    assert(index < _size);
    T old;
    do {
      old = _array[index].load();
    } while (!_array[index].compare_exchange_weak(old, t));
  }

  T operator[](size_t index) const { 
	  assert(index < _size);
	  return _array[index].load(); 
  }

  bool insert(const T &t) {
    size_t oldPos;
    size_t newPos;
    do {
      oldPos = _pos.load();
      newPos = oldPos + 1;
      if (oldPos >= _size) {
        return false;
      }
    } while (!_pos.compare_exchange_weak(oldPos, newPos));
    _array[oldPos].store(t);
    return true;
  }

  size_t cap() const { return _size - _pos.load(); }

protected:
  size_t _size;
  std::atomic_size_t _pos;
  std::atomic<T> *_array;
};
}
}
