#include <chrono>
#include <cmath>
#include <iostream>
#include <map>
#include <numeric>
#include <thread>
#include <vector>

#include <libdariadb/ads/fixed_tree.h>
#include <libdariadb/meas.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/exception.h>

#include <boost/date_time/posix_time/posix_time.hpp>
#include <stx/btree_map>

const dariadb::Time FROM = 0;
const size_t WRITES_PER_SEC = 5;
const dariadb::Time STEP =
    boost::posix_time::seconds(1).total_milliseconds() / WRITES_PER_SEC;
const dariadb::Time TO = STEP * 1000000;

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
    default: // millisecond
      return WRITES_PER_SEC;
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
    result[5] = dt.millisecond / (uint16_t)STEP;
    // std::cout << result[0] << ' ' << result[1] << ' ' << result[2] << ' ' << result[3]
    // << ' ' << result[4] << ' ' << result[5] << '\n';
    return result;
  }
};

template <typename T> struct Statistic {
  void append(const T &t) {}
};

using TimeTree = dariadb::ads::FixedTree<dariadb::Time, dariadb::Meas, KeySplitter,
                                         Statistic<dariadb::Meas>>;

void one_thread_bench(dariadb::Time from, dariadb::Time to, dariadb::Time step) {

  auto m = dariadb::Meas::empty();
  size_t count = (to - from) / step;
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

  std::cout << std::endl << "stx::btree_map: one thread benchmark..." << std::endl;
  stx::btree_map<dariadb::Time, dariadb::Meas> meas_bmap;
  {
    auto start = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    for (auto i = from; i < to; i += step) {
      m.time = i;
      meas_bmap.insert(std::make_pair(i, m));
    }
    auto end = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    auto elapsed = end.count() - start.count();
    std::cout << "write: " << elapsed << " ms" << std::endl;
    std::cout << "speed: " << count / elapsed * 1000 << " per sec." << std::endl;

    start = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    for (auto i = from; i < to; i += step) {
      meas_bmap.find(i);
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
void write_thread(TimeTree *tree, size_t num, dariadb::Time from, dariadb::Time to,
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

void read_thread(TimeTree *tree, size_t num, dariadb::Time from, dariadb::Time to,
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
  const size_t count = (TO - FROM) / STEP;

  std::cout << "from:" << FROM << " to:" << TO << " step:" << STEP
            << " total:" << (TO - FROM) / STEP << std::endl;

  one_thread_bench(FROM, TO, STEP);
  one_thread_bench_time(FROM, TO, STEP);

  std::cout << std::endl << "Multi thread benchmark..." << std::endl;
  const size_t threads_count = 5;
  std::vector<std::thread> threads;
  threads.resize(threads_count);
  elapsed_times.resize(threads_count);
  TimeTree tree;
  for (size_t i = 0; i < threads_count; ++i) {
    auto t = std::thread{write_thread, &tree, i, FROM, TO, STEP};
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
    auto t = std::thread{read_thread, &tree, i, FROM, TO, STEP};
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
