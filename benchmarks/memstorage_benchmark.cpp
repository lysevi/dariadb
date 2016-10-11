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
	typedef std::array<size_t, levels_count> splited_key;
	size_t level_size(size_t level_num) const {
		auto res = std::pow(2, sizeof(uint8_t) * 8);
		return res;
	}

	splited_key split(const T &k) const {
		splited_key result;
		auto in_bts = reinterpret_cast<const uint8_t *>(&k);
		for (size_t i = 0; i < levels_count; ++i) {
			result[levels_count - i - 1] = in_bts[i];
		}
		return result;
	}
};

struct KeySplitter {
  static const size_t levels_count = size_t(6);
  typedef std::array<size_t, levels_count> splited_key;
  size_t level_size(size_t level_num) const {
    switch (level_num) {
    case 0: // year
      return 2500;
    case 1: // day of year
      return 366;
    case 2: // hour
      return 24;
    case 3: // minute
      return 60;
    case 4: // second
      return 60;
	case 5: // millisecond
		return 1000;
    }
  }

  splited_key split(const dariadb::Time &k) const {
    auto dt = dariadb::timeutil::to_datetime(k);

    splited_key result;
    result[0] = dt.year;
    result[1] = dt.day_of_year;
    result[2] = dt.hour;
    result[3] = dt.minute;
    result[4] = dt.second;
	result[5] = dt.millisecond;
    return result;
  }
};

template <typename T> struct Statistic {
  void append(const T &t) {}
};

using TestTree = dariadb::ads::FixedTree<dariadb::Time, dariadb::Meas, ByteKeySplitter<dariadb::Time>,
                                         Statistic<dariadb::Meas>>;

using TimeTree = dariadb::ads::FixedTree<dariadb::Time, dariadb::Meas, KeySplitter,
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

void one_thread_bench_time(dariadb::Time from, dariadb::Time to, dariadb::Time step) {
	std::cout << "\nFixedTree_Time: one thread benchmark..." << std::endl;
	TimeTree tree;
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
  one_thread_bench_time(from, to, step);

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
