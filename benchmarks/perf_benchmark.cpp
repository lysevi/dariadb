#include <cstdlib>
#include <ctime>
#include <iostream>
#include <iterator>

#include "bench_common.h"
#include <dariadb.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <ctime>
#include <limits>
#include <random>
#include <storage/capacitor.h>
#include <storage/memstorage.h>
#include <thread>
#include <utils/fs.h>

std::atomic_long append_count{0};
bool stop_info = false;

class BenchCallback : public dariadb::storage::ReaderClb {
public:
  BenchCallback() { count = 0; }
  void call(const dariadb::Meas &) { count++; }
  size_t count;
};

void show_info(dariadb::storage::Engine *storage) {
  clock_t t0 = clock();
  auto all_writes =
      dariadb_bench::total_threads_count * dariadb_bench::iteration_count;
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    clock_t t1 = clock();
    auto writes_per_sec =
        append_count.load() / double((t1 - t0) / CLOCKS_PER_SEC);
    auto queue_sizes = storage->queue_size();
    std::cout << "\rin memory chunks: " << storage->chunks_in_memory()
              << " in disk chunks: "
              << dariadb::storage::PageManager::instance()->chunks_in_cur_page()
              << " in queue: (p:" << queue_sizes.page
              << " m:" << queue_sizes.mem << " cap:" << queue_sizes.cap << ")"
              << " pooled: "
              << dariadb::storage::ChunkPool::instance()->polled_chunks()
              << " writes: " << append_count << " speed: " << writes_per_sec
              << "/sec progress:" << (int64_t(100) * append_count) / all_writes
              << "%                ";
    std::cout.flush();
    if (stop_info) {
      std::cout.flush();
      break;
    }
  }
  std::cout << "\n";
}

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;
  std::cout << "Performance benchmark" << std::endl;
  const std::string storage_path = "testStorage";
  {
    std::cout << "write..." << std::endl;

    const size_t chunk_per_storage = 1024 * 1024;
    const size_t chunk_size = 256;
    const size_t cap_B = 128 * 1024 / chunk_size;
    const dariadb::Time old_mem_chunks = 0;
    const size_t max_mem_chunks = 100;

    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    auto start_time = dariadb::timeutil::current_time();

    auto raw_ptr = new dariadb::storage::Engine(
        dariadb::storage::PageManager::Params(storage_path,
                                              chunk_per_storage, chunk_size),
        dariadb::storage::Capacitor::Params(cap_B, storage_path),
        dariadb::storage::Engine::Limits(old_mem_chunks, max_mem_chunks));

    dariadb::storage::BaseStorage_ptr ms{raw_ptr};

    append_count = 0;
    stop_info = false;

    std::thread info_thread(show_info, raw_ptr);

    std::vector<std::thread> writers(dariadb_bench::total_threads_count);

    size_t pos = 0;
    for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
      std::thread t{dariadb_bench::thread_writer_rnd_stor, dariadb::Id(pos),
                    dariadb::Time(i), &append_count, ms};
      writers[pos++] = std::move(t);
    }

    pos = 0;
    for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
      std::thread t = std::move(writers[pos++]);
      t.join();
    }

    stop_info = true;
    info_thread.join();

    {
      std::cout << "full flush..." << std::endl;
      auto start = clock();
      raw_ptr->flush();
      auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
      std::cout << "flush time: " << elapsed << std::endl;
    }

    auto queue_sizes = raw_ptr->queue_size();
    std::cout << "\rin memory chunks: " << raw_ptr->chunks_in_memory()
              << " in disk chunks: "
              << dariadb::storage::PageManager::instance()->chunks_in_cur_page()
              << " in queue: (p:" << queue_sizes.page
              << " m:" << queue_sizes.mem << " cap:" << queue_sizes.cap << ")"
              << " pooled: "
              << dariadb::storage::ChunkPool::instance()->polled_chunks()
              << std::endl;

    {
      std::cout << "time point reads..." << std::endl;
      std::random_device r;
      std::default_random_engine e1(r());
      std::uniform_int_distribution<dariadb::Id> uniform_dist(ms->minTime(),
                                                              ms->maxTime());

      std::shared_ptr<BenchCallback> clbk{new BenchCallback};

      auto start = clock();

      const size_t reads_count = 10;
      for (size_t i = 0; i < reads_count; i++) {
        auto time_point = uniform_dist(e1);
        ms->readInTimePoint(time_point)->readAll(clbk.get());
      }
      auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC) / reads_count;
      std::cout << "time: " << elapsed << std::endl;
    }
    {
      std::cout << "intervals reads..." << std::endl;
      std::random_device r;
      std::default_random_engine e1(r());
      std::uniform_int_distribution<dariadb::Id> uniform_dist(ms->minTime(),
                                                              ms->maxTime());

      std::shared_ptr<BenchCallback> clbk{new BenchCallback};

      auto start = clock();

      const size_t reads_count = 10;
      for (size_t i = 0; i < reads_count; i++) {
        auto time_point1 = uniform_dist(e1);
        auto time_point2 = uniform_dist(e1);
        auto from = std::min(time_point1, time_point2);
        auto to = std::max(time_point1, time_point2);
        ms->readInterval(from, to)->readAll(clbk.get());
      }
      auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC) / reads_count;
      std::cout << "time: " << elapsed << std::endl;
    }
    {
      std::cout << "read all..." << std::endl;
      std::shared_ptr<BenchCallback> clbk{new BenchCallback()};
      auto start = clock();
      ms->readInterval(start_time, ms->maxTime())->readAll(clbk.get());

      auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
      std::cout << "readed: " << clbk->count << std::endl;
      std::cout << "time: " << elapsed << std::endl;
      if (clbk->count != (dariadb_bench::iteration_count *
                          dariadb_bench::total_threads_count)) {
        throw MAKE_EXCEPTION(
            "(clbk->count!=(iteration_count*total_threads_count))");
      }
    }
    std::cout << "stoping storage...\n";
    ms = nullptr;
  }
  std::cout << "cleaning...\n";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}
