#include <chrono>
#include <iostream>
#include <libdariadb/ads/radix.h>
#include <libdariadb/meas.h>
#include <map>

template <class T> struct KeySplitter {
  static const size_t levels_count = sizeof(T);
  typedef std::array<size_t, levels_count> splited_key;
  size_t level_size(size_t level_num) const {
    auto res = std::pow(2, sizeof(uint8_t) * 8);
    return res;
  }

  splited_key split(const T &k) const {
    splited_key result;
    auto in_bts = reinterpret_cast<const uint8_t *>(&k);
    for (size_t i = 0; i < levels_count; ++i) {
      result[i] = in_bts[i];
    }
    return result;
  }
};

void one_thread_bench(dariadb::Time from, dariadb::Time to, dariadb::Time step) {
  std::cout << "RadixPlusTree: one thread benchmark..." << std::endl;
  using TestTree = dariadb::ads::RadixPlusTree<dariadb::Time, dariadb::Meas,
                                               KeySplitter<dariadb::Time>>;
  TestTree tree;
  auto m = dariadb::Meas::empty();
  {
	  auto start = std::chrono::duration_cast<std::chrono::milliseconds>(
		  std::chrono::system_clock::now().time_since_epoch());
	  for (auto i = from; i < to; i += step) {
		  m.time = i;
		  tree.insert(i, m);
	  }
	  auto end = std::chrono::duration_cast<std::chrono::milliseconds>(
		  std::chrono::system_clock::now().time_since_epoch());
	  auto elapsed = end.count() - start.count();
	  std::cout << "write: "<<elapsed << " ms" << std::endl;

	  start = std::chrono::duration_cast<std::chrono::milliseconds>(
		  std::chrono::system_clock::now().time_since_epoch());
	  for (auto i = from; i < to; i += step) {
		  m.time = i;
		  tree.find(i);
	  }
	  end = std::chrono::duration_cast<std::chrono::milliseconds>(
		  std::chrono::system_clock::now().time_since_epoch());
	  elapsed = end.count() - start.count();
	  std::cout << "read: " << elapsed << " ms" << std::endl;
	  std::cout << "midle:"<< elapsed/((to-from)/step) << " ms" << std::endl;
  }
  

 /* std::cout << "std::map: one thread benchmark..." << std::endl;
  std::map<dariadb::Time, dariadb::Meas> meas_map;
  {
	  auto start = std::chrono::duration_cast<std::chrono::milliseconds>(
		  std::chrono::system_clock::now().time_since_epoch());
	  for (auto i = from; i < to; i += step) {
		  m.time = i;
		  meas_map.insert(std::make_pair(i, m));
	  }
	  auto end = std::chrono::duration_cast<std::chrono::milliseconds>(
		  std::chrono::system_clock::now().time_since_epoch());
	  auto elapsed = end.count() - start.count();
	  std::cout << "write: " << elapsed << " ms" << std::endl;

	  start = std::chrono::duration_cast<std::chrono::milliseconds>(
		  std::chrono::system_clock::now().time_since_epoch());
	  for (auto i = from; i < to; i += step) {
		  m.time = i;
		  meas_map.find(i);
	  }
	  end = std::chrono::duration_cast<std::chrono::milliseconds>(
		  std::chrono::system_clock::now().time_since_epoch());
	  elapsed = end.count() - start.count();
	  std::cout << "read: " << elapsed << " ms" << std::endl;
	  std::cout << "midle:" << elapsed / ((to - from) / step) << " ms" << std::endl;
  }*/
}

int main(int argc, char **argv) {
  one_thread_bench(0, 100000, 2);
}
