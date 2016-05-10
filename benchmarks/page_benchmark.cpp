#include <cstdlib>
#include <ctime>
#include <iostream>
#include <iterator>

#include <dariadb.h>
#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <ctime>
#include <limits>
#include <random>
#include <storage/page_manager.h>
#include <thread>
#include <utils/fs.h>

class BenchCallback : public dariadb::storage::Cursor::Callback {
public:
  void call(dariadb::storage::Chunk_Ptr &) { count++; }
  size_t count;
};

const std::string storagePath = "benchStorage/";
const size_t chunks_count = 1024;
const size_t chunks_size = 1024;

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;
  {
    dariadb::Time t = 0;

    dariadb::Meas first;
    first.id = 1;
    first.time = t;

    dariadb::storage::Chunk_Ptr ch =
        std::make_shared<dariadb::storage::ZippedChunk>(chunks_size, first);
    for (int i = 0;; i++, t++) {
      first.flag = dariadb::Flag(i);
      first.time = t;
      first.value = dariadb::Value(i);
      if (!ch->append(first)) {
        break;
      }
    }

    if (dariadb::utils::fs::path_exists(storagePath)) {
      dariadb::utils::fs::rm(storagePath);
    }
    dariadb::storage::PageManager::start(dariadb::storage::PageManager::Params(
        storagePath, dariadb::storage::MODE::SINGLE, chunks_count,
        chunks_size));

    auto start = clock();

    for (size_t i = 0; i < chunks_count; ++i) {
      dariadb::storage::PageManager::instance()->append(ch);
      ch->info.maxTime += dariadb::Time(chunks_size);
      ch->info.minTime += dariadb::Time(chunks_size);
    }

    auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "insert : " << elapsed << std::endl;
    dariadb::storage::PageManager::instance()->flush();
    std::random_device r;
    std::default_random_engine e1(r());
    std::uniform_int_distribution<dariadb::Id> uniform_dist(
        dariadb::storage::PageManager::instance()->minTime(),
        dariadb::storage::PageManager::instance()->maxTime());

    BenchCallback *clbk = new BenchCallback;

    start = clock();

    for (size_t i = 0; i < size_t(100); i++) {
      auto time_point1 = uniform_dist(e1);
      auto time_point2 = uniform_dist(e1);
      auto from = std::min(time_point1, time_point2);
      auto to = std::max(time_point1, time_point2);
      auto cursor = dariadb::storage::PageManager::instance()->chunksByIterval(
          dariadb::IdArray{}, dariadb::Flag(0), from, to);
      cursor->readAll(clbk);
      cursor = nullptr;
    }

    elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "interval: " << elapsed << std::endl;

    start = clock();

    for (size_t i = 0; i < size_t(100); i++) {
      auto time_point = uniform_dist(e1);
      dariadb::storage::PageManager::instance()->chunksBeforeTimePoint(
          dariadb::IdArray{}, 0, time_point);
    }

    elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "timePoint: " << elapsed << std::endl;

    delete clbk;

    dariadb::storage::PageManager::stop();
    if (dariadb::utils::fs::path_exists(storagePath)) {
      dariadb::utils::fs::rm(storagePath);
    }
    ch = nullptr;
  }
}
