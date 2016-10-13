#pragma once

#include <libdariadb/ads/lockfree_array.h>
#include <array>
#include <list>
#include <tuple>
#include <type_traits>
#include <vector>

namespace dariadb {
namespace ads {

template <typename K, typename V, typename KeySplitter, typename Statistic>
class FixedTree {
  static_assert(std::is_default_constructible<Statistic>::value,
                "Statistic must be trivially constructible.");
  static_assert(std::is_default_constructible<KeySplitter>::value,
                "KeySplitter must be trivially constructible.");
  static_assert(std::is_pod<V>::value || std::is_copy_assignable<V>::value,
                "Value must be POD or copy-assign implemented.");

public:
  struct Node {
    using KV = std::pair<size_t, V>;
    using Node_Ptr = Node *;
    Node(FixedTree *_rdt, size_t _level, size_t _size)
        : childs(_size), level(_level), size(_size), rdt(_rdt), stat() {}

    Node(FixedTree *_rdt, size_t _level, size_t _size, bool _is_leaf)
        : childs(), level(_level), size(_size), rdt(_rdt), _leaf_values(_size),
          is_leaf(_is_leaf), stat() {}

    ~Node() {
      for (size_t i = 0; i < childs.size(); ++i) {
        delete childs[i];
      }
    }

    bool childExists(size_t index) { return childs[index] != nullptr; }

    Node_Ptr get(size_t index) const { return childs[index]; }

    Node_Ptr create_or_get(size_t index) {
      auto old = childs[index];
      if (old != nullptr) {
        return old;
      }
      auto new_level_num = level + 1;
      Node_Ptr new_child = nullptr;

      if (new_level_num == KeySplitter::levels_count - 1) {
        new_child = new Node(rdt, new_level_num, rdt->level_size(new_level_num), true);
      } else {
        new_child = new Node(rdt, new_level_num, rdt->level_size(new_level_num));
      }

      childs.compare_exchange(index, old, new_child);
      if (old != nullptr) {
        delete new_child;
        return old;
      } else {
        return new_child;
      }
    }
    void append(const size_t key, const V &v) {
      this->_leaf_values[key] = std::make_pair(key, v);
    }
    LockFreeArray<Node_Ptr> childs;
    size_t level;
    size_t size;
    FixedTree *rdt;
    std::vector<KV> _leaf_values;
    bool is_leaf;

    Statistic stat;
  };

  FixedTree() {
    _head = new Node(this, size_t(0), this->level_size(0));
    _keys_count.store(size_t(0));
  }
  ~FixedTree() { delete _head; }

  size_t keys_count() const { return _keys_count; }
  size_t level_size(size_t level_num) const { return _splitter.level_size(level_num); }

  void insert(const K &k, const V &v) {
    typename KeySplitter::splited_key splited_k = _splitter.split(k);
    size_t pos = 0;
    auto cur = _head;
    for (; pos < KeySplitter::levels_count - 1; ++pos) {
      cur = cur->create_or_get(splited_k[pos]);
      cur->stat.append(v);
      assert(cur != nullptr);
    }
    assert(cur->is_leaf);
    if (cur->is_leaf) {
      cur->append(splited_k[pos], v);
    }
    _keys_count += 1;
  }

  bool find(const K &k, V *out) const {
    typename KeySplitter::splited_key splited_k = _splitter.split(k);
    size_t pos = 0;
    auto cur = _head;
    for (; pos < KeySplitter::levels_count - 1; ++pos) {
      cur = cur->get(splited_k[pos]);
      if (cur == nullptr) {
        return false;
      }
    }
    if (cur->is_leaf) {
      auto v = cur->_leaf_values[splited_k[pos]];
      *out = v.second;
    }
    return true;
  }

protected:
  KeySplitter _splitter;
  Node *_head;
  std::atomic_size_t _keys_count;
};
}
}
