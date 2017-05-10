#pragma once

#include <libdariadb/utils/jenkins_hash.h>
#include <atomic>
#include <mutex>
#include <type_traits>
#include <utility>

namespace dariadb {
namespace utils {

template <typename _Key, typename _Data, typename _Value = std::pair<_Key, _Data>,
          typename _Locker = std::mutex, typename _Hash = jenkins_one_hash<_Key>>
class stripped_map {
public:
  typedef _Key key_type;
  typedef _Data data_type;
  typedef _Value value_type;

  typedef _Locker locker_type;
  typedef _Hash hash_type;

  struct bucket_t;
  typedef bucket_t *bucket_ptr;
  struct bucket_t {
    size_t hash;
    value_type _kv;
    bucket_ptr next;
  };

  typedef bucket_ptr *_buckets_container;
  typedef stripped_map<_Key, _Data, _Value, _Locker, _Hash> self_type;

  struct iterator {
    locker_type *locker;
    value_type *v;

    iterator() {
      locker = nullptr;
      v = nullptr;
    }

    iterator(const iterator &other) = delete;
    iterator &operator=(const iterator &other) = delete;

    iterator(iterator &&other) : locker(other.locker), v(other.v) {
      other.locker = nullptr;
      other.v = nullptr;
    }

    iterator &operator=(iterator &&other) {
      locker = other.locker;
      v = other.v;
      other.locker = nullptr;
      other.v = nullptr;
      return *this;
    }

    ~iterator() {
      if (locker != nullptr) {
        locker->unlock();
      }
    }
  };

  static const size_t default_n = 1000;
  static const size_t grow_coefficient = 2;

  stripped_map(const size_t N)
      : _lockers(nullptr), _buckets(nullptr), _size(size_t(0)), _N(N), _L(N) {
    reserve(_N);
    static_assert(std::is_default_constructible<data_type>::value,
                  "Value must be is_default_constructible");
    static_assert(std::is_default_constructible<key_type>::value,
                  "Key must be is_default_constructible");
  }

  stripped_map() : stripped_map(default_n) {}

  stripped_map(const self_type &other) = delete;
  self_type &operator=(const self_type &other) = delete;

  ~stripped_map() {
    clear();

    delete[] _buckets;
    _buckets = nullptr;

    delete[] _lockers;
    _lockers = nullptr;

    _N = size_t(0);
    _size.store(size_t());
  }

  void reserve(size_t N) {
    clear();
    _lockers = new locker_type[N];
    _buckets = new bucket_ptr[N];
    std::fill_n(_buckets, N, nullptr);
    _N = N;
    _L = N;
  }

  void clear() {
    if (_buckets == nullptr) {
      return;
    }
    this->lock_all();

    for (size_t i = 0; i < _N; ++i) {
      auto bucket = _buckets[i];
      if (bucket != nullptr) {
        for (auto it = bucket; it != nullptr;) {
          auto next = it->next;
          delete it;
          it = next;
        }
      }
	  _buckets[i] = nullptr;
    }

    this->unlock_all();
  }

  size_t size() const { return _size.load(); }
  bool empty() const { return _size.load() == size_t(0); }

  bool find(const key_type &k, data_type *output) {
    auto hash = hasher(k);

    auto lock_index = hash % _L;
    _lockers[lock_index].lock();

    auto bucket_index = hash % _N;
    bucket_ptr bucket = _buckets[bucket_index];

    auto result = false;
    if (bucket != nullptr) {
      for (bucket_ptr iter = bucket; iter != nullptr; iter = iter->next) {
        if (iter->_kv.first == k) {
          *output = iter->_kv.second;
          result = true;
          break;
        }
      }
    }
    _lockers[lock_index].unlock();

    return result;
  }

  void insert(const key_type &_k, const data_type &_v) {
    static_assert(std::is_trivially_copyable<key_type>::value,
                  "Key must be trivial copyable");
    auto pos = find_bucket(_k);
    pos.v->second = _v;
  }

  iterator find_bucket(const key_type &_k) {
    rehash();
    iterator result;

    auto hash = hasher(_k);

    auto lock_index = hash % _L;
    auto target_locker = &(_lockers[lock_index]);
    target_locker->lock();
    result.locker = target_locker;

    auto bucket_index = hash % _N;

    find_bucket(result, _buckets, bucket_index, hash, _k);
    return result;
  }

  void rehash() {
    const double max_load_factor = 1.0;

    auto lf = load_factor();

    if (lf > max_load_factor) { // rehashing
      lock_all();

      lf = load_factor();
      if (lf > max_load_factor) {
        auto old_N = _N;
        _N = _N * grow_coefficient;

        _buckets_container new_buckets = new bucket_ptr[_N];
        std::fill_n(new_buckets, _N, nullptr);

        iterator tmp_result;
        for (size_t i = 0; i < old_N; ++i) {
          auto l = _buckets[i];
          if (l != nullptr) {
            for (auto source_iter = l; source_iter != nullptr;) {
              auto next = source_iter->next;
              source_iter->next = nullptr;

              auto hash = source_iter->hash;
              auto bucket_index = hash % _N;

              bucket_ptr target_bucket = new_buckets[bucket_index];
              if (target_bucket == nullptr) {
                new_buckets[bucket_index] = source_iter;
                new_buckets[bucket_index]->next = nullptr;
              } else {
                for (auto target_iter = target_bucket; target_iter != nullptr;
                     target_iter = target_iter->next) {
                  if (target_iter->next != nullptr &&
                      target_iter->next->_kv.first > source_iter->_kv.first) {

                    auto target_next = target_iter->next;
                    source_iter->next = target_next;
                    target_iter->next = source_iter;
                    break;
                  }
                  if (target_iter->next == nullptr) {
                    target_iter->next = source_iter;
                    break;
                  }
                }
              }

              source_iter = next;
            }
          }
        }
        delete[] _buckets;
        _buckets = new_buckets;
      }

      unlock_all();
    }
  }

  void apply(std::function<void(const value_type &v)> func) const {
    for (size_t i = 0; i < _N; ++i) {
      auto locker_index = i % _L;
      auto target_locker = &_lockers[locker_index];
      target_locker->lock();
      auto l = _buckets[i];
      if (l != nullptr) {
        for (bucket_ptr iter = l; iter != nullptr; iter = iter->next) {
          func(iter->_kv);
        }
      }
      target_locker->unlock();
    }
  }

  double load_factor() const { return double(_size.load()) / _N; }

  size_t N() const { return _N; }

protected:
  void lock_all() const {
    for (size_t i = 0; i < _L; ++i) {
      _lockers[i].lock();
    }
  }

  void unlock_all() const {
    for (size_t i = _L - 1;; --i) {
      _lockers[i].unlock();
      if (i == 0) {
        break;
      }
    }
  }

  void find_bucket(iterator &result, _buckets_container target, size_t bucket_index,
                   const size_t hash, const key_type &_k) {
    bucket_ptr bucket = target[bucket_index];
    if (bucket == nullptr) {
      bucket_ptr new_item = new bucket_t;
      new_item->_kv.first = _k;
      new_item->hash = hash;
      new_item->next = nullptr;

      target[bucket_index] = new_item;
      result.v = &new_item->_kv;
      _size.fetch_add(size_t(1));
      return;
    }

    for (auto iter = bucket; iter != nullptr; iter = iter->next) {
      if (iter->_kv.first == _k) {
        result.v = &iter->_kv;
        return;
      } else {
        if (iter->next != nullptr && iter->next->_kv.first > _k) {
          auto next = iter->next;

          bucket_ptr new_item = new bucket_t;
          new_item->_kv.first = _k;
          new_item->hash = hash;
          new_item->next = next;

          iter->next = new_item;
          result.v = &new_item->_kv;
          _size.fetch_add(size_t(1));
          return;
        }
        if (iter->next == nullptr) {
          bucket_ptr new_item = new bucket_t;
          new_item->_kv.first = _k;
          new_item->hash = hash;
          new_item->next = nullptr;

          iter->next = new_item;
          result.v = &new_item->_kv;
          _size.fetch_add(size_t(1));
          return;
        }
      }
    }
    return;
  }

private:
  mutable locker_type *_lockers;
  _buckets_container _buckets;
  std::atomic_size_t _size;
  size_t _N;
  size_t _L;
  hash_type hasher;
};
} // namespace utils
} // namespace dariadb