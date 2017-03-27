#pragma once

#include <algorithm>
#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <type_traits>
#include <utility>
#include <vector>

namespace dariadb {
namespace utils {

template <typename _Key, typename _Data, typename _Value = std::pair<_Key, _Data>,
          typename _Locker = std::mutex, typename _Hash = std::hash<_Key>>
class stripped_map {
public:
  typedef _Key key_type;
  typedef _Data data_type;
  typedef _Value value_type;

  typedef _Locker locker_type;
  typedef _Hash hash_type;
  typedef std::vector<value_type> bucket_type;
  typedef std::vector<bucket_type> _buckets_container;
  typedef stripped_map<_Key, _Data, _Value, _Locker, _Hash> self_type;

  static const size_t default_n = 100;
  static const size_t grow_coefficient = 2;
  static const size_t max_load_factor = 4;

  stripped_map(const size_t N) : _lockers(N), _buckets(N), _size(size_t(0)), _N(N) {
    static_assert(std::is_default_constructible<data_type>::value,
                  "Value must be is_default_constructible");
    static_assert(std::is_default_constructible<key_type>::value,
                  "Key must be is_default_constructible");
    for (auto &b : _buckets) {
      reserve_buckets(b);
    }
  }

  stripped_map() : stripped_map(default_n) {}

  stripped_map(const self_type &other) = delete;
  self_type &operator=(const self_type &other) = delete;

  ~stripped_map() {
    for (auto &l : _lockers) {
      l.lock();
    }
    _buckets.clear();
    _size.store(size_t());

    for (auto &l : _lockers) {
      l.unlock();
    }
    _lockers.clear();
  }

  void reserve(size_t N) {
    _lockers = std::move(std::vector<locker_type>(N));
    _buckets.resize(N);
    for (auto &b : _buckets) {
      reserve_buckets(b);
    }
    _N = N;
  }

  void clear() {
    for (auto it = _lockers.begin(); it != _lockers.end(); ++it) {
      it->lock();
    }

    _buckets.clear();
    _N = size_t(0);

    for (auto it = _lockers.rbegin(); it != _lockers.rend(); ++it) {
      it->unlock();
    }
  }

  size_t size() const { return _size.load(); }
  bool empty() const { return _size.load() == size_t(0); }

  bool find(const key_type &k, data_type *output) {
    auto hash = hasher(k);

    auto lock_index = hash % _lockers.size();
    _lockers[lock_index].lock();

    auto bucket_index = hash % _N;
    auto bucket = &_buckets[bucket_index];

    auto result = false;
    for (auto kv : *bucket) {
      if (kv.first > k) {
        break;
      }
      if (kv.first == k) {
        *output = kv.second;
        result = true;
        break;
      }
    }
    _lockers[lock_index].unlock();

    return result;
  }

  struct iterator {
    locker_type *locker;
    value_type *v;

    iterator() {
      locker = nullptr;
      v = nullptr;
    }

    iterator(const iterator &other) = delete;
    iterator(const iterator &&other) = delete;
    iterator &operator=(const iterator &other) = delete;

    ~iterator() { locker->unlock(); }
  };

  using iterator_ptr = std::shared_ptr<iterator>;

  void insert(const key_type &_k, const data_type &_v) {
    static_assert(std::is_trivially_copyable<key_type>::value,
                  "Key must be trivial copyable");
    auto pos = insertion_pos(_k);
    pos->v->second = _v;
  }

  iterator_ptr insertion_pos(const key_type &_k) {
    rehash();
    iterator_ptr result = std::make_shared<iterator>();

    auto hash = hasher(_k);

    auto lock_index = hash % _lockers.size();
    auto target_locker = &(_lockers[lock_index]);
    target_locker->lock();
    result->locker = target_locker;

    auto bucket_index = hash % _N;
    auto target_bucket = &_buckets[bucket_index];

    bool is_update = false;
    bool is_inserted = false;
    if (!target_bucket->empty()) {
      auto kv = std::make_pair(_k, data_type());
      auto iter = std::lower_bound(target_bucket->begin(), target_bucket->end(), kv,
                                   [](auto v1, auto v2) { return v1.first < v2.first; });
      if (iter != target_bucket->end()) {
        if (iter->first == _k) {
          is_update = true;
          result->v = &(*iter);
        } else {
          is_inserted = true;
          auto kv = std::make_pair(_k, data_type());
          auto insertion_iterator = target_bucket->insert(iter, kv);
          result->v = &(*insertion_iterator);
        }
      }
    }

    if (!is_update) {
      _size.fetch_add(size_t(1));
      if (!is_inserted) {
        target_bucket->push_back(std::make_pair(_k, data_type()));
        result->v = &(target_bucket->back());
      }
    }

    return result;
  }

  void rehash() {
    auto lf = load_factor();

    if (lf > max_load_factor) { // rehashing
      lock_all();

      lf = load_factor();
      if (lf > max_load_factor) {
        _N = _N * grow_coefficient;

        _buckets_container new_buckets(_N);
        for (auto &b : new_buckets) {
          reserve_buckets(b);
        }
        for (auto l : _buckets) {
          for (auto v : l) {
            auto hash = hasher(v.first);
            auto bucket_index = hash % _N;
            new_buckets[bucket_index].push_back(v);
          }
        }
        _buckets = std::move(new_buckets);
      }

      unlock_all();
    }
  }

  void apply(std::function<void(const value_type &v)> func) const {
    lock_all();
    for (auto &l : _buckets) {
      for (auto &v : l) {
        func(v);
      }
    }
    unlock_all();
  }

  size_t load_factor() const { return _size.load() / _N; }

  size_t N() const { return _N; }

protected:
  void reserve_buckets(bucket_type &b) { b.reserve(max_load_factor); }

  void lock_all() const {
    for (auto it = _lockers.begin(); it != _lockers.end(); ++it) {
      it->lock();
    }
  }

  void unlock_all() const {
    for (auto it = _lockers.rbegin(); it != _lockers.rend(); ++it) {
      it->unlock();
    }
  }

private:
  mutable std::vector<locker_type> _lockers;
  _buckets_container _buckets;
  std::atomic_size_t _size;
  size_t _N;

  hash_type hasher;
}; // namespace utils
} // namespace utils
} // namespace dariadb