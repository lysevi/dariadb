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
  const size_t K = 1;
  const size_t id_count = 4;
  {

    if (dariadb::utils::fs::path_exists(storagePath)) {
      dariadb::utils::fs::rm(storagePath);
    }
    dariadb::storage::PageManager::start(dariadb::storage::PageManager::Params(
        storagePath, chunks_count, chunks_size));

    auto start = clock();
    auto m = dariadb::Meas::empty();

    for (size_t i = 0; i < K * 1000000; i++) {
      m.id = i % id_count;
      m.flag = 0xff;
      m.time = dariadb::timeutil::current_time();
      m.value = dariadb::Value(i);
      dariadb::storage::PageManager::instance()->append(m);
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
    dariadb::IdArray ids(id_count);
    for (size_t i = 0; i < id_count; ++i) {
      ids[i] = i;
    }

    start = clock();

    for (size_t i = 0; i < size_t(100); i++) {
      auto time_point1 = uniform_dist(e1);
      auto time_point2 = uniform_dist(e1);
      auto from = std::min(time_point1, time_point2);
      auto to = std::max(time_point1, time_point2);
      auto link_list = dariadb::storage::PageManager::instance()->chunksByIterval(
          dariadb::storage::QueryInterval(ids, 0, from, to));
	  auto cursor = dariadb::storage::PageManager::instance()->readLinks(link_list);
      cursor->readAll(clbk);
      assert(clbk->count != 0);
      clbk->count = 0;
      cursor = nullptr;
    }

    elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "interval: " << elapsed << std::endl;

    start = clock();

    for (size_t i = 0; i < size_t(100); i++) {
      auto time_point = uniform_dist(e1);
      dariadb::storage::PageManager::instance()->chunksBeforeTimePoint(
          dariadb::storage::QueryTimePoint(ids, 0, time_point));
    }

    elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "timePoint: " << elapsed << std::endl;

    delete clbk;

    dariadb::storage::PageManager::stop();
    if (dariadb::utils::fs::path_exists(storagePath)) {
      dariadb::utils::fs::rm(storagePath);
    }
  }
}
