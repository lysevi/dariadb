#pragma once

#include <iostream>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <list>
#include <sstream>
#include <vector>
#include <tuple>
namespace dariadb {
namespace utils {

class cascading {
public:
  struct item {
    bool is_init;
    int value;
    item();
    item(int v);
    std::string to_string() const;
    bool operator<(const item &other) const;
    bool operator>(const item &other) const;

    bool operator==(const item &other) const{
        return std::tie(this->is_init,this->value)==std::tie(other.is_init,other.value);
    }
  };

  struct level {
    std::vector<item> values;
    bool is_clean;
    size_t pos;
    size_t size;
    size_t lvl;
    item minItem,maxItem;

    level() = default;

    level(size_t _size, size_t lvl_num);
    void clear();
    bool free();
    bool is_full();
    std::string to_string() const;
    void insert(item val);
    void merge_with(std::list<level *> new_values);
    bool find(int v, item*i);
  };
  void alloc_level(size_t num);

public:
  cascading() : _items_count(0), _next_level(0) {}

  void resize(size_t levels_count);

  void push(int v);

  void print() {
    std::cout << "==:\n";
    for (size_t i = 0; i < _levels.size(); ++i) {
      std::cout << _levels[i].to_string() << std::endl;
    }
    std::cout << std::endl;
  }
  size_t levels_count() const { return _levels.size(); }
  bool find(int v, item*res);
protected:
  std::vector<level> _levels;
  size_t _items_count;
  size_t _next_level;
};
}
}
