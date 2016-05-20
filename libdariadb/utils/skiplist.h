#pragma once
#include "cz.h"
#include <algorithm>
#include <cstddef>
#include <functional>
#include <iterator>
#include <limits>
#include <random>
#include <utility>
#include <vector>

namespace dariadb {
namespace utils {

template <typename Key, typename Value, typename _Compare = std::less<Key>,
          typename RndEngine = std::mt19937>
class skiplist {
public:
  using key_type = Key;
  using value_type = Value;
  using pair_type = std::pair<key_type, value_type>;
  using cmp_type = _Compare;

  struct _node_t;
  using _node_t_ptr = _node_t *;
  using _node_frwd_vector = std::vector<_node_t_ptr>;
  struct _node_t {
    _node_frwd_vector frwd;
    pair_type kv;
    size_t level;
  };

  class iterator {
  public:
    typedef iterator self_type;
    typedef pair_type value_type;
    typedef pair_type &reference;
    typedef pair_type *pointer;
    typedef std::forward_iterator_tag iterator_category;
    typedef ptrdiff_t difference_type;

    _node_t *_node;

    explicit iterator(_node_t *x) : _node(x) {}
    iterator(const iterator &other) : _node(other._node) {}
    ~iterator() { _node = nullptr; }
    void swap(iterator &other) noexcept { std::swap(_node, other._node); }

    self_type &operator=(const iterator &other) {
      iterator tmp(other);
      this->swap(tmp);
      return *this;
    }

    value_type operator*() const { return _node->kv; }
    reference &operator*() { return _node->kv; }
    pointer operator->() const { return &_node->kv; }

    bool operator==(const iterator &other) const {
      return _node == other._node;
    }

    inline bool operator!=(const iterator &other) const {
      return _node != other._node;
    }

    /// ++Prefix
    inline self_type &operator++() {
      if (_node->frwd[0] != nullptr) {
        _node = _node->frwd[0];
      }
      return *this;
    }
    /// Postfix++
    inline self_type operator++(int) {
      iterator tmp = *this;

      if (_node->frwd[0] != nullptr) {
        _node = _node->frwd[0];
      }
      return tmp;
    }
  };

  class const_iterator {
  public:
    typedef const_iterator self_type;
    typedef const pair_type value_type;
    typedef const pair_type &reference;
    typedef const pair_type *pointer;
    typedef std::forward_iterator_tag iterator_category;
    typedef ptrdiff_t difference_type;

    _node_t const *_node;

    explicit const_iterator(const _node_t *x) : _node(x) {}
    const_iterator(const const_iterator &other) : _node(other._node) {}
    ~const_iterator() { _node = nullptr; }
    void swap(const_iterator &other) noexcept { std::swap(_node, other._node); }

    self_type &operator=(const const_iterator &other) {
      const_iterator tmp(other);
      this->swap(other);
    }
    value_type operator*() const { return _node->kv; }
    reference &operator*() { return _node->kv; }
    pointer operator->() const { return &_node->kv; }

    bool operator==(const const_iterator &other) const {
      return _node == other._node;
    }

    inline bool operator!=(const const_iterator &other) const {
      return _node != other._node;
    }

    /// ++Prefix
    inline self_type &operator++() {
      if (_node->frwd[0] != nullptr) {
        _node = _node->frwd[0];
      }
      return *this;
    }
    /// Postfix++
    inline self_type operator++(int) {
      const_iterator tmp = *this;

      if (_node->frwd[0] != nullptr) {
        _node = _node->frwd[0];
      }
      return tmp;
    }
  };

public:
  skiplist(size_t maxLevel) : _size(0), dist(0, 1.0), _maxLevel(maxLevel) {
    _head.frwd.resize(_maxLevel);
    _tail.frwd.resize(_maxLevel);
    for (size_t i = 0; i < _maxLevel; ++i) {
      _head.frwd[i] = &_tail;
      _tail.frwd[i] = nullptr;
    }
    _head.level = 0;
    _tail.kv.first = std::numeric_limits<key_type>::max();
  }

  skiplist() : skiplist(32) {}

  ~skiplist() {
    _node_t_ptr x = _head.frwd[0];
    while (x != &_tail) {
      auto saved = x;
      x = x->frwd[0];
      delete saved;
    }
  }

  size_t size() const { return _size; }

  void insert(const Key &k, const Value &v) { insert(std::make_pair(k, v)); }

  //void print() {
  //  for (auto it = cbegin(); it != cend(); ++it) {
  //    std::cout << " {" << it->first << ':' << it->second << ":"
  //              << it._node->level << "}";
  //  }
  //  std::cout << "\n";
  //}

  void insert(const pair_type &kv) {

    _node_frwd_vector update(_maxLevel);
    _node_t_ptr x = search(kv.first, &update);
    if (x->kv.first == kv.first) {
      x->kv = kv;
    } else {
      ++_size;
      auto lvl = randomLevel();
      if (lvl > _head.level) {
        for (auto i = _head.level + 1; i < lvl; ++i) {
          update[i] = &_head;
        }
        _head.level = lvl;
      }
      x = makeNode(lvl, kv);
      for (size_t i = 0; i < lvl; ++i) {
        x->frwd[i] = update[i]->frwd[i];
        update[i]->frwd[i] = x;
      }
    }
  }

  void remove(const key_type &key) {
    _node_frwd_vector update(_maxLevel);
    _node_t_ptr x = search(key, &update);

    if (x->kv.first == key) {
      for (size_t i = 0; i < _head.level; ++i) {
        if (update[i]->frwd[i] != x) {
          break;
        }
        update[i]->frwd[i] = x->frwd[i];
      }
      delete x;
      --_size;
      while (_head.level > 0 && _head.frwd[_head.level] == nullptr) {
        --_head.level;
      }
    }
  }

  template <class IterT, class PredT>
  void remove_if(IterT from, IterT to, PredT pred) {
    for (auto it = from; it != to;) {
      if (pred(it._node->kv)) {
        auto next = it++;
        this->remove(next->first);
      } else {
        ++it;
      }
    }
  }

  iterator begin() { return iterator{_head.frwd[0]}; }
  iterator end() { return iterator{&_tail}; }
  const_iterator cbegin() const { return const_iterator{_head.frwd[0]}; }
  const_iterator cend() const { return const_iterator(&_tail); }

  iterator find(const key_type &key) {
    _node_frwd_vector update(_maxLevel);
    _node_t_ptr x = search(key, &update);

    if (x->kv.first == key) {
      return iterator(x);
    } else {
      return iterator(&_tail);
    }
  }

  const_iterator find(const key_type &key) const {
    _node_frwd_vector update(_maxLevel);
    _node_t_ptr x = search(key, &update);

    if (x->kv.first == key) {
      return iterator(x);
    } else {
      return iterator(&_tail);
    }
  }

  iterator upper_bound(const key_type &key) {
    _node_frwd_vector update(_maxLevel);
    _node_t_ptr x = search(key, &update);
    iterator res(x);
    if (res->first == key) {
      ++res;
    }
    return res;
  }

  const_iterator upper_bound(const key_type &key) const {
    _node_frwd_vector update(_maxLevel);
    _node_t_ptr x = search(key, &update);
    const_iterator res(x);
    if (res->first == key) {
      ++res;
    }
    return res;
  }

  iterator lower_bound(const key_type &key) {
    _node_frwd_vector update(_maxLevel);
    _node_t_ptr x = search_before(key, &update);
    iterator res(x);
    return res;
  }
  const_iterator lower_bound(const key_type &key) const {
    _node_frwd_vector update(_maxLevel);
    _node_t_ptr x = search_before(key, &update);
    const_iterator res(x);
    return res;
  }

private:
  _node_t_ptr search_before(const key_type &key, _node_frwd_vector *update) {
    _node_t_ptr x = &_head;
    for (size_t i = _head.level;; --i) {
      while (_cmp(x->frwd[i]->kv.first, key)) {
        x = x->frwd[i];
      }
      // x->key < searchKey <= x->forward[i]->key
      (*update)[i] = x;
      if (i == 0) {
        break;
      }
    }
    return x;
  }

  _node_t_ptr search(const key_type &key, _node_frwd_vector *update) {
    auto x = search_before(key, update);
    x = x->frwd[0];
    return x;
  }

  size_t randomLevel() const {
    size_t lvl = 1;
    while (dist(rd) < p && lvl < (_maxLevel - 1)) {
      ++lvl;
    }
    return lvl;
  }

  _node_t_ptr makeNode(const size_t lvl, const pair_type &kv) const {
    _node_t *result = new _node_t;
    result->frwd.resize(lvl);
    result->kv = kv;
    result->level = lvl;
    result->kv = kv;
    return result;
  }

protected:
  const double p = 0.5;

  _node_t _head, _tail;
  size_t _size;
  cmp_type _cmp;
  mutable RndEngine rd;
  mutable std::uniform_real_distribution<double> dist;
  size_t _maxLevel;
};
}
}
