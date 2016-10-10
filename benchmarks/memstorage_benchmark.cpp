#include <chrono>
#include <cmath>
#include <iostream>
#include <libdariadb/ads/fixed_tree.h>
#include <libdariadb/meas.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/exception.h>
#include <map>
#include <numeric>
#include <thread>
#include <vector>

template <class T> struct ByteKeySplitter {
	static const size_t levels_count = sizeof(T);
	static const size_t levels_size = 256;
	typedef std::array<size_t, levels_count> splited_key;

	splited_key split(const T &k) const {
		splited_key result;
		auto in_bts = reinterpret_cast<const uint8_t *>(&k);
		for (size_t i = 0; i < levels_count; ++i) {
			result[levels_count - i - 1] = in_bts[i];
		}
		return result;
	}
};

template <typename T> struct Statistic {
  void append(const T &t) {}
};

using TestTree = dariadb::ads::FixedTree<dariadb::Time, dariadb::Meas, ByteKeySplitter<dariadb::Time>,
                                         Statistic<dariadb::Meas>>;

void one_thread_bench(dariadb::Time from, dariadb::Time to, dariadb::Time step) {
  std::cout << "FixedTree: one thread benchmark..." << std::endl;
  TestTree tree;
  auto m = dariadb::Meas::empty();
  size_t count = (to - from) / step;
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
    std::cout << "write: " << elapsed << " ms" << std::endl;
    std::cout << "speed: " << count / elapsed * 1000 << " per sec." << std::endl;

    start = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    for (auto i = from; i < to; i += step) {
      tree.find(i, &m);
    }
    end = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    elapsed = end.count() - start.count();
    std::cout << "read: " << elapsed << " ms" << std::endl;
    std::cout << "midle: " << double(elapsed) / ((to - from) / step) << " ms"
              << std::endl;
    std::cout << "speed: " << count / elapsed * 1000 << " per sec." << std::endl;
  }

  std::cout << std::endl << "std::map: one thread benchmark..." << std::endl;
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
    std::cout << "speed: " << count / elapsed * 1000 << " per sec." << std::endl;

    start = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    for (auto i = from; i < to; i += step) {
      meas_map.find(i);
    }
    end = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    elapsed = end.count() - start.count();
    std::cout << "read: " << elapsed << " ms" << std::endl;
    std::cout << "midle: " << double(elapsed) / ((to - from) / step) << " ms"
              << std::endl;
    std::cout << "speed: " << count / elapsed * 1000 << " per sec." << std::endl;
  }
}

std::vector<size_t> elapsed_times;
void write_thread(TestTree *tree, size_t num, dariadb::Time from, dariadb::Time to,
                  dariadb::Time step) {
  auto m = dariadb::Meas::empty();
  auto start = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());
  for (auto i = from; i < to; i += step) {
    m.time = i;
    tree->insert(i, m);
  }
  auto end = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());
  auto elapsed = end.count() - start.count();
  elapsed_times[num] = elapsed;
}

void read_thread(TestTree *tree, size_t num, dariadb::Time from, dariadb::Time to,
                 dariadb::Time step) {
  auto m = dariadb::Meas::empty();
  auto start = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());

  for (auto i = from; i < to; i += step) {
    auto flag = tree->find(i, &m);
    if (!flag) {
      THROW_EXCEPTION("read error: key-", i, " reader-", num);
    }
  }

  auto end = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());
  auto elapsed = end.count() - start.count();
  elapsed_times[num] = elapsed;
}

int main(int argc, char **argv) {
  const dariadb::Time from = 0;
  const dariadb::Time to = 1000000;
  const dariadb::Time step = 1;
  const size_t count = (to - from) / step;
  one_thread_bench(from, to, step);

  std::cout << std::endl << "Multi thread benchmark..." << std::endl;
  const size_t threads_count = 5;
  std::vector<std::thread> threads;
  threads.resize(threads_count);
  elapsed_times.resize(threads_count);
  TestTree tree;
  for (size_t i = 0; i < threads_count; ++i) {
    auto t = std::thread{write_thread, &tree, i, from, to, step};
    threads[i] = std::move(t);
  }

  for (size_t i = 0; i < threads_count; ++i) {
    threads[i].join();
  }

  auto average_time =
      std::accumulate(elapsed_times.begin(), elapsed_times.end(), 0.0) / threads_count;
  std::cout << "write average time: " << average_time << " sec." << std::endl;
  std::cout << "write average speed: " << count / average_time * 1000 << " per sec."
            << std::endl;

  for (size_t i = 0; i < threads_count; ++i) {
    auto t = std::thread{read_thread, &tree, i, from, to, step};
    threads[i] = std::move(t);
  }

  for (size_t i = 0; i < threads_count; ++i) {
    threads[i].join();
  }

  average_time =
      std::accumulate(elapsed_times.begin(), elapsed_times.end(), 0.0) / threads_count;
  std::cout << "read average time: " << average_time << " sec." << std::endl;
  std::cout << "read average speed: " << count / average_time * 1000 << " per sec."
            << std::endl;
}
