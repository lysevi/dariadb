#pragma once

#include "locker.h"
#include <type_traits>
namespace dariadb {
namespace utils {

template <typename K, typename V> 
class RadixPlusTree {
  static_assert(std::is_pod<V>::value || std::is_copy_assignable<V>::value,
                "Value must be POD or copy-assign implemented.");

  struct Node{

  };
public:
  RadixPlusTree() {}
  size_t keys_count() const { return 0; }

protected:
};
}
}