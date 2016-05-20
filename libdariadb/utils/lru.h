#pragma once

#include <algorithm>
#include <deque>
#include <functional>
#include <list>

namespace dariadb {
namespace utils {
template <class Key, class Value, class EqualPred = std::equal_to<Key>>
class LRU {
public:
  using pair_t = std::pair<Key, Value>;

  LRU(const size_t max_size):_max_size(max_size) {
  }
  
  void set_max_size(const size_t max_size) {
	  _max_size = max_size;
  }

  void erase(const Key&key) {
	  for (auto it = _deq.begin(); it != _deq.end(); ++it) {
		  if (EqualPred()(it->first, key)) {
			  _deq.erase(it);
			  break;
		  }
	  }
  }

  /// return true, if write to dropped value;
  bool put(const Key &k, const Value &v, Value*dropped) {
	  inner_put(k, v);
	if (_deq.size() > _max_size) {
		*dropped = _deq.back().second;
		_deq.pop_back();
		return true;
	}
	return false;
  }

  bool find(const Key &k, Value *out)  {
    auto fres = std::find_if(_deq.begin(), _deq.end(), [&k](const pair_t &p) {
      return EqualPred()(p.first, k);
    });
    if (fres == _deq.end()) {
      return false;
    } else {
      *out = (*fres).second;
	  _deq.erase(fres);
	  inner_put(k, *out);
      return true;
    }
  }

  size_t size()const { return _deq.size(); }

  void clear() {
	  _deq.clear();
  }
protected:
	inline void inner_put(const Key&k, const Value &v){
          _deq.push_front(std::make_pair(k, v));
    }
protected:
  std::deque<pair_t> _deq;
  size_t _max_size;
};
}
}