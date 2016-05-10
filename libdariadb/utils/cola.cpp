#include "cola.h"
#include "cz.h"

using namespace dariadb::utils;

cascading::item::item() {
  is_init = false;
  value = 0;
}

cascading::item::item(int v) {
  is_init = true;
  value = v;
}

std::string cascading::item::to_string() const {
  std::stringstream ss;
  ss << value;
  return ss.str();
}

bool cascading::item::operator<(const item &other) const {
  return value < other.value;
}

bool cascading::item::operator>(const item &other) const {
  return value > other.value;
}

cascading::level::level(size_t _size, size_t lvl_num) {
  size = _size;
  values.resize(size);
  lvl = lvl_num;
  this->clear();
}

void cascading::level::clear() {
  //            for(size_t i=0;i<values.size();i++){
  //                values[i]=item();
  //            }
  is_clean = true;
  pos = 0;

  minItem.value = std::numeric_limits<int>::max();
  maxItem.value = std::numeric_limits<int>::min();
}

bool cascading::level::free() { return (size - pos) != 0; }

bool cascading::level::is_full() { return !free(); }

std::string cascading::level::to_string() const {
  std::stringstream ss;
  ss << lvl << ": [";
  for (size_t j = 0; j < size; ++j) {
    ss << values[j].to_string() << " ";
  }
  ss << "]";
  return ss.str();
}

void cascading::level::insert(item val) {
  if (pos < size) {
    values[pos] = val;
    pos++;

    is_clean = false;
    if (val > this->maxItem) {
      maxItem = val;
    }

    if (val < this->minItem) {
      minItem = val;
    }
  }
}

void cascading::level::merge_with(std::list<level *> new_values) {
  std::vector<size_t> poses(new_values.size());
  std::fill(poses.begin(), poses.end(), size_t(0));
  while (!new_values.empty()) {
    // get cur max;
    size_t with_max_index = 0;
    item max_val = new_values.front()->values[poses[0]];
    auto it = new_values.begin();
    auto with_max_index_it = it;
    for (size_t i = 0; i < poses.size(); i++, ++it) {
      if (max_val > (*it)->values[poses[i]]) {
        with_max_index = i;
        max_val = (*it)->values[poses[i]];
        with_max_index_it = it;
      }
    }

    this->insert((*with_max_index_it)->values[poses[with_max_index]]);
    // remove ended in-list
    poses[with_max_index]++;
    if (poses[with_max_index] >= (*with_max_index_it)->values.size()) {
      poses.erase(poses.begin() + with_max_index);
      new_values.erase(with_max_index_it);
    }
  }
}

bool cascading::level::find(int v, item *i) {
  auto res =
      std::lower_bound(this->values.begin(), this->values.end(), item(v));
  if (res != this->values.end()) {
    if ((res->value > v) && (res != values.begin())) {
      --res;
    }
    if (res->value == v) {
      *i = *res;
      return true;
    }
  }
  return false;
}

void cascading::alloc_level(size_t num) {
  auto nr_ent = size_t(1 << num);
  _levels.push_back(level(nr_ent, num));
  _next_level++;
}

void cascading::resize(size_t levels_count) {
  for (size_t i = 0; i < levels_count; ++i) {
    alloc_level(i);
  }
}

void cascading::push(int v) {
  size_t new_items_count = _items_count + 1;
  size_t outlvl = ctz(~_items_count & new_items_count);
  // size_t mrg_k=outlvl+1; //k-way merge: k factor
  // std::cout<<"outlvl: "<<uint32_t(outlvl)<<" k:"<<mrg_k <<std::endl;

  if (new_items_count == size_t(1 << _next_level)) {
    // std::cout<<"allocate new level: "<<_next_level<<std::endl;
    alloc_level(_next_level);
  }

  std::list<level *> to_merge;
  level tmp(1, 0);
  tmp.insert(v);
  to_merge.push_back(&tmp);

  for (size_t i = 1; i <= outlvl; ++i) {
    to_merge.push_back(&_levels[i - 1]);
  }

  auto merge_target = &_levels[outlvl];
  merge_target->merge_with(to_merge);
  for (auto l : to_merge) {
    l->clear();
  }
  ++_items_count;
}

bool cascading::find(int v, item *res) {
  for (auto it = _levels.begin(); it != _levels.end(); ++it) {
    if (it->free()) {
      continue;
    }
    if ((v >= it->minItem.value) && (v <= it->maxItem.value)) {
      if (it->find(v, res)) {
        return true;
      }
    }
  }
  return false;
}
