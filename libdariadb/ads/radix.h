#pragma once

#include "lockfree_array.h"
#include <array>
#include <list>
#include <tuple>
#include <type_traits>

namespace dariadb {
namespace ads {

template <typename K, typename V, typename KeySplitter,
          typename NodeContainer = std::list<V>>
class RadixPlusTree {

  static_assert(std::is_default_constructible<NodeContainer>::value,
                "NodeContainer must be trivially constructible.");
  static_assert(std::is_default_constructible<KeySplitter>::value,
                "KeySplitter must be trivially constructible.");
  static_assert(std::is_pod<V>::value || std::is_copy_assignable<V>::value,
                "Value must be POD or copy-assign implemented.");

public:
  struct Node {
    using Node_Ptr = Node *;
    Node(RadixPlusTree *_rdt, size_t _level, size_t _size)
        : childs(_size), level(_level), size(_size), rdt(_rdt) {}
    ~Node() {
      for (size_t i = 0; i < childs.size(); ++i) {
        delete childs[i];
      }
    }

    bool childExists(size_t index) { return childs[index] != nullptr; }

    const Node_Ptr get(size_t index) const { return childs[index]; }

    Node_Ptr create_or_get(size_t index) {
      auto old = childs[index];
      if (old != nullptr) {
        return old;
      }
      auto new_level_num = level + 1;
      auto new_child = new Node(rdt, new_level_num, rdt->level_size(new_level_num));

      childs.compare_exchange(index, old, new_child);
      if (old != nullptr) {
        delete new_child;
        return old;
      } else {
        return new_child;
      }
    }
    void append(const V &v) { this->values.push_back(v); }
    LockFreeArray<Node_Ptr> childs;
    size_t level;
    size_t size;
    RadixPlusTree *rdt;
    NodeContainer values;
  };

  RadixPlusTree() {
    _head = new Node(this, size_t(0), this->level_size(0));
    _keys_count.store(size_t(0));
  }
  ~RadixPlusTree() { delete _head; }

  size_t keys_count() const { return _keys_count; }
  size_t level_size(size_t level_num) const { return _splitter.level_size(level_num); }

  void insert(const K &k, const V &v) {
    typename KeySplitter::splited_key splited_k = _splitter.split(k);
    size_t pos = 0;
    auto cur = _head;
    for (; pos < KeySplitter::levels_count; ++pos) {
      cur = cur->create_or_get(splited_k[pos]);
    }
    cur->append(v);
    _keys_count += 1;
  }

  NodeContainer find(const K &k) const {
    NodeContainer result;
	typename KeySplitter::splited_key splited_k = _splitter.split(k);
    size_t pos = 0;
    auto cur = _head;
    for (; pos < KeySplitter::levels_count; ++pos) {
      cur = cur->get(splited_k[pos]);
      if (cur == nullptr) {
        return result;
      }
    }
    for (auto v : cur->values) {
      result.push_back(v);
    }
    return result;
  }

protected:
  KeySplitter _splitter;
  Node *_head;
  std::atomic_size_t _keys_count;
};
}
}