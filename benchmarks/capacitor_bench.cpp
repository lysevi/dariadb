#include <cstdlib>
#include <ctime>
#include <iostream>
#include <iterator>

#include <ctime>
#include <storage/capacitor.h>
#include <timeutil.h>
#include <utils/fs.h>
#include "bench_common.h"

std::atomic_long append_count{0};
bool stop_info = false;

class Moc_Storage : public dariadb::storage::MeasWriter {
public:
  dariadb::append_result append(const dariadb::Meas &) override {
    return dariadb::append_result(1, 0);
  }

  void flush() override {}
};

void show_info() {
  clock_t t0 = clock();
  auto all_writes =
      dariadb_bench::total_threads_count * dariadb_bench::iteration_count;
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    clock_t t1 = clock();
    auto writes_per_sec =
        append_count.load() / double((t1 - t0) / CLOCKS_PER_SEC);

    std::cout << "\r"
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

  {
    const std::string storage_path = "testStorage";
    const size_t cap_B = 128 * 1024 / sizeof(dariadb::Meas);
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
    auto *tos=new dariadb::storage::Capacitor(
        stor.get(), dariadb::storage::Capacitor::Params(cap_B, storage_path));

    dariadb::storage::MeasStorage_ptr meas_stor(tos);
    std::thread info_thread(show_info);

    std::vector<std::thread> writers(dariadb_bench::total_threads_count);

    size_t pos = 0;
    for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
      std::thread t{dariadb_bench::thread_writer_rnd_stor, dariadb::Id(pos),
                    dariadb::Time(i), &append_count, meas_stor};
      writers[pos++] = std::move(t);
    }

    pos = 0;
    for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
      std::thread t = std::move(writers[pos++]);
      t.join();
    }

    stop_info = true;
    info_thread.join();

  }
}
