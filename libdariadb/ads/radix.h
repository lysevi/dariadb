#pragma once

#include "../utils/locker.h"
#include "lockfree_array.h"
#include <type_traits>

namespace dariadb {
namespace ads {

template <typename K, typename V> 
class RadixPlusTree {
  static_assert(std::is_pod<V>::value || std::is_copy_assignable<V>::value,
                "Value must be POD or copy-assign implemented.");
public:
  struct Node{
	  using Node_Ptr = Node*;
	  Node(size_t _level, size_t _size):
		  level(_level),
		  childs(_size) {
	  }
	  ~Node() {
		  for (size_t i = 0; i < childs.size(); ++i) {
			  delete childs[i];
		  }
	  }
	  bool childExists(size_t index) {
		  return childs[index] != nullptr;
	  }

	  Node_Ptr create_or_get(size_t index) {
		  auto old = childs[index];
		  if (old != nullptr) {
			  return old;
		  }
		  auto new_child = new Node(level, childs.size());
		  
		  childs.compare_exchange(index, old, new_child);
		  if (old != nullptr) {
			  delete new_child;
			  return old;
		  }
		  else {
			  return new_child;
		  }
	  }
	  LockFreeArray<Node_Ptr> childs;
	  size_t level;
  };

  RadixPlusTree() {}
  size_t keys_count() const { return 0; }

protected:
};
}
}