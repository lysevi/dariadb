#include <algorithm>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <map>
#include <random>
#include <stx/btree_map.h>
#include <utils/exception.h>
#include <utils/skiplist.h>
#include <vector>

const size_t insertion_count = 1000000;

template <class T> void remove_map_less(T *cont) {
  bool dropped = true;
  while (dropped) {
    dropped = false;
    for (auto i = cont->begin(); i != cont->end(); ++i) {
      if ((*i).first < (insertion_count * 0.75)) {
        cont->erase(i);
        dropped = true;
        break;
      }
    }
  }
}

template <typename T> void kv_rnd_benchmark(T &cont, std::vector<int> &keys) {
  auto start = clock();
  for (size_t i = 0; i < insertion_count; i++) {
    cont.insert(std::make_pair(keys[i], i + 1));
  }
  auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
  std::cout << "write rnd: " << elapsed;

  start = clock();
  for (size_t i = 0; i < insertion_count; i++) {
    cont.find(keys[i]);
  }
  elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
  std::cout << " read: " << elapsed << std::endl;
}

template <typename T>
void kv_linear_benchmark(T &cont, std::function<void(T *)> rm_func) {
  auto start = clock();
  for (size_t i = 0; i < insertion_count; i++) {
    cont.insert(std::make_pair(i, i + 1));
  }
  auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
  std::cout << "write linear: " << elapsed;

  start = clock();
  for (size_t i = 0; i < insertion_count; i++) {
    cont.find(i);
  }
  elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
  std::cout << " read: " << elapsed;

  start = clock();
  rm_func(&cont);
  elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
  std::cout << " remove: " << elapsed << std::endl;
}

template <typename T> void kv_linear_benchmark_rev(T &cont) {
  auto start = clock();
  for (size_t i = insertion_count; i > 0; i--) {
    cont.insert(std::make_pair(i, i + 1));
  }
  auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
  std::cout << "write revlinear: " << elapsed;
}

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;

  std::vector<int> keys(insertion_count);
  std::uniform_int_distribution<int> distribution(0, insertion_count * 10);
  std::mt19937 engine;
  auto generator = std::bind(distribution, engine);
  std::generate_n(keys.begin(), insertion_count, generator);

  {
    std::cout << "skiplist " << std::endl;
    using int_lst = dariadb::utils::skiplist<int, int>;
    {
      int_lst lst;
      kv_rnd_benchmark(lst, keys);
    }
    {
      int_lst lst;
      std::function<void(int_lst *)> f = [](int_lst *il) {
        il->remove_if(il->begin(), il->end(), [](const int_lst::pair_type &p) {
          return p.first < (insertion_count * 0.75);
        });
      };
      kv_linear_benchmark(lst, f);
    }
    {
      int_lst lst;
      kv_linear_benchmark_rev(lst);
    }
  }
  {
    std::cout << "\nstd::map " << std::endl;
    {
      std::map<int, int> lst;
      kv_rnd_benchmark(lst, keys);
    }
    {
      std::map<int, int> lst;
      std::function<void(std::map<int, int> *)> f =
          &remove_map_less<std::map<int, int>>;
      kv_linear_benchmark(lst, f);
    }
    {
      std::map<int, int> lst;
      kv_linear_benchmark_rev(lst);
    }
  }

  {
    std::cout << "\nstx::btree_map " << std::endl;
    {
      stx::btree_map<int, int> lst;
      kv_rnd_benchmark(lst, keys);
    }
    {
      stx::btree_map<int, int> lst;
      std::function<void(stx::btree_map<int, int> *)> f =
          &remove_map_less<stx::btree_map<int, int>>;
      kv_linear_benchmark(lst, f);
    }
    {
      stx::btree_map<int, int> lst;
      kv_linear_benchmark_rev(lst);
    }
  }
}
