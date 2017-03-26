#pragma once

#include <libdariadb/utils/async/locker.h>
#include <atomic>
#include <list>
#include <type_traits>
#include <utility>
#include <vector>

namespace dariadb {
namespace utils {

template <typename _Key, typename _Data, typename _Value = std::pair<_Key, _Data>,
          typename _Locker = async::Locker, typename _Hash = std::hash<_Key>>
class stripped_map {
public:
  typedef _Key key_type;
  typedef _Data data_type;
  typedef _Value value_type;

  typedef _Locker locker_type;
  typedef _Hash hash_type;
  typedef std::list<value_type> bucket_type; // TODO replace by
                                             // std::ordered_queue<value_type> with
                                             // pri-resize in ctor (use load factor)
  typedef std::vector<bucket_type> _buckets_container;
  typedef stripped_map<_Key, _Data, _Value, _Locker, _Hash> self_type;

  static const size_t default_n = 10;
  static const size_t grow_coefficient = 2;

  stripped_map(const size_t N) : _lockers(N), _buckets(N), _size(size_t(0)), _N(N) {
    static_assert(std::is_default_constructible<data_type>::value,
                  "Value must be is_default_constructible");
    static_assert(std::is_default_constructible<key_type>::value,
                  "Key must be is_default_constructible");
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

  size_t size() const { return _size.load(); }

  bool get(const key_type &k, data_type *output) {
    auto hash = hasher(k);

    auto lock_index = hash % _lockers.size();
    _lockers[lock_index].lock();

    auto bucket_index = hash % _N;
    auto bucket = &_buckets[bucket_index];

    auto result = false;
    for (auto kv : *bucket) {
      if (kv.first == k) {
        *output = kv.second;
        result = true;
        break;
      }
    }
    _lockers[lock_index].unlock();

    return result;
  }

  void insert(const key_type &k, const data_type &v) {
    static_assert(std::is_trivially_copyable<data_type>::value,
                  "Value must be trivial copyable");
    static_assert(std::is_trivially_copyable<key_type>::value,
                  "Key must be trivial copyable");
    const double max_load_factor = 2;

    auto hash = hasher(k);

    auto lock_index = hash % _lockers.size();
    _lockers[lock_index].lock();

    auto bucket_index = hash % _N; 
	//TODO insert if key not exists. otherwise, replace value.
    _buckets[bucket_index].push_back(value_type(k, v));

    _lockers[lock_index].unlock();
    auto lf = load_factor();
    _size.fetch_add(size_t(1));

    if (lf > max_load_factor) { // rehashing
      for (auto it = _lockers.begin(); it != _lockers.end(); ++it) {
        it->lock();
      }

      lf = load_factor();
      if (lf > max_load_factor) {
        _N = _N * grow_coefficient;

        _buckets_container new_buckets(_N);

        for (auto l : _buckets) {
          for (auto v : l) {
            hash = hasher(v.first);
            bucket_index = hash % _N;
            new_buckets[bucket_index].push_back(v);
          }
        }
        _buckets = std::move(new_buckets);
      }

      for (auto it = _lockers.rbegin(); it != _lockers.rend(); ++it) {
        it->unlock();
      }
    }
  }

  double load_factor() const { return _size.load() / _N; }

  size_t N() const { return _N; }

private:
  std::vector<locker_type> _lockers;
  _buckets_container _buckets;
  std::atomic_size_t _size;
  size_t _N;

  hash_type hasher;
};
}
}