#pragma once

namespace dariadb {
namespace utils {
/**
T - at(size_t)
Out - push_back(T)
*/
template <class T, class Out, class comparer_t>
void k_merge(std::list<T *> new_values, Out &out, comparer_t comparer) {
  auto vals_size = new_values.size();
  std::list<size_t> poses;
  for (size_t i = 0; i < vals_size; ++i) {
    poses.push_back(0);
  }
  while (!new_values.empty()) {
    vals_size = new_values.size();
    // get cur max;
    auto with_max_index = poses.begin();
    auto max_val = new_values.front()->at(*with_max_index);
    auto it = new_values.begin();
    auto with_max_index_it = it;
    for (auto pos_it = poses.begin(); pos_it != poses.end(); ++pos_it) {
      if (!comparer(max_val, (*it)->at(*pos_it))) {
        with_max_index = pos_it;
        max_val = (*it)->at(*pos_it);
        with_max_index_it = it;
      }
      ++it;
    }

    auto val = (*with_max_index_it)->at(*with_max_index);
    if(out.size()== 0 || out.back()!=val){
        out.push_back(val);
    }
    // remove ended in-list
    (*with_max_index)++;
    if ((*with_max_index) >= (*with_max_index_it)->size()) {
      poses.erase(with_max_index);
      new_values.erase(with_max_index_it);
    }
  }
}
}
}
