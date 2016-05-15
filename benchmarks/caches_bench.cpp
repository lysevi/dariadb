#include <cstdlib>
#include <ctime>
#include <iostream>
#include <iterator>

#include "bench_common.h"
#include <dariadb.h>
#include <atomic>
#include <chrono>
#include <cmath>
#include <ctime>
#include <limits>
#include <storage/capacitor.h>
#include <storage/memstorage.h>
#include <thread>
#include <utils/fs.h>

class BenchCallback : public dariadb::storage::ReaderClb {
public:
  void call(const dariadb::Meas &) { count++; }
  size_t count;
};

class Moc_Storage : public dariadb::storage::MeasStorage {
public:
  dariadb::append_result append(const dariadb::Meas &) override{
    return dariadb::append_result(1, 0);
  }

  dariadb::Time minTime() override { return 0; }
  /// max time of writed meas
  dariadb::Time maxTime() override { return 0; }

  void subscribe(const dariadb::IdArray &, const dariadb::Flag &,
                 const dariadb::storage::ReaderClb_ptr &) override {}
  dariadb::storage::Reader_ptr currentValue(const dariadb::IdArray &,
                                            const dariadb::Flag &)override {
    return nullptr;
  }

  void flush() override {}


  dariadb::IdArray getIds() { return dariadb::IdArray{}; }

  // Inherited via MeasStorage
  virtual dariadb::storage::Reader_ptr
  readInterval(const dariadb::storage::QueryInterval &q) override {
    return nullptr;
  }
  virtual dariadb::storage::Reader_ptr
  readInTimePoint(const dariadb::storage::QueryTimePoint &q) override {
    return nullptr;
  }
};

std::atomic_long append_count{0};
bool stop_info = false;

void show_info() {
  clock_t t0 = clock();
  auto all_writes =
      dariadb_bench::total_threads_count * dariadb_bench::iteration_count;
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    clock_t t1 = clock();
    auto writes_per_sec =
        append_count.load() / double((t1 - t0) / CLOCKS_PER_SEC);
    // auto read_per_sec = read_all_times.load() / double((t1 - t0) /
    // CLOCKS_PER_SEC);
    std::cout << "\rwrites: " << writes_per_sec
              << "/sec progress:" << (int64_t(100) * append_count) / all_writes
              << "%     ";
    /*if (!stop_read_all) {
            std::cout << " read_all_times: " << read_per_sec <<"/sec ";
    }*/
    std::cout.flush();
    if (stop_info) {
      /*std::cout << "\rwrites: " << writes_per_sec
              << "/sec progress:" << (100 * append_count) / all_writes
              << "%            ";
      if (!stop_read_all) {
              std::cout << " read_all_times: " << read_per_sec << "/sec ";
      }*/
      std::cout.flush();
      break;
    }
  }
  std::cout << "\n";
}

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;

  {
    std::cout << "Capacitor" << std::endl;
    const std::string storage_path = "testStorage";
    const size_t chunk_size = 256;
    const size_t cap_B = 128 * 1024 / chunk_size;
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    dariadb::storage::MeasStorage_ptr ms{new Moc_Storage()};
    std::unique_ptr<dariadb::storage::Capacitor> cp{
        new dariadb::storage::Capacitor(
            ms.get(),
            dariadb::storage::Capacitor::Params(cap_B, storage_path))};

    append_count = 0;
    stop_info = false;

    std::thread info_thread(show_info);
    std::vector<std::thread> writers(dariadb_bench::total_threads_count);
    size_t pos = 0;
    for (size_t i = 0; i < dariadb_bench::total_threads_count; i++) {
      std::thread t{dariadb_bench::thread_writer_rnd_stor, i, dariadb::Time(i),
                    &append_count, ms};
      writers[pos++] = std::move(t);
    }

    pos = 0;
    for (size_t i = 0; i < dariadb_bench::total_threads_count; i++) {
      std::thread t = std::move(writers[pos++]);
      t.join();
    }
    stop_info = true;
    info_thread.join();
    cp->flush();
  }

  {
    std::cout << "Union" << std::endl;
    const std::string storage_path = "testStorage";
    const size_t chunk_per_storage = 1024;
    const size_t chunk_size = 512;
    const size_t cap_B = 1024;
    const dariadb::Time old_mem_chunks = 0;
    const size_t max_mem_chunks = 0;

    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    dariadb::storage::MeasStorage_ptr ms{new dariadb::storage::Engine(
        dariadb::storage::PageManager::Params(storage_path, chunk_per_storage,
                                              chunk_size),
        dariadb::storage::Capacitor::Params(cap_B, storage_path),
        dariadb::storage::Engine::Limits(max_mem_chunks, old_mem_chunks))};

    append_count = 0;
    stop_info = false;

    std::thread info_thread(show_info);
    std::vector<std::thread> writers(dariadb_bench::total_threads_count);

    size_t pos = 0;
    for (size_t i = 0; i < dariadb_bench::total_threads_count; i++) {
      std::thread t{dariadb_bench::thread_writer_rnd_stor, i, dariadb::Time(i),
                    &append_count, ms};
      writers[pos++] = std::move(t);
    }

    pos = 0;
    for (size_t i = 0; i < dariadb_bench::total_threads_count; i++) {
      std::thread t = std::move(writers[pos++]);
      t.join();
    }
    stop_info = true;
    info_thread.join();
    // read_all_t.join();
    ms = nullptr;
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }
  }
}
