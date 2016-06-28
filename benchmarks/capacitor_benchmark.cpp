#include <cstdlib>
#include <ctime>
#include <iostream>
#include <iterator>
#include <thread>

#include "bench_common.h"
#include <ctime>
#include <storage/capacitor_manager.h>
#include <storage/manifest.h>
#include <timeutil.h>
#include <utils/fs.h>

#include <boost/program_options.hpp>

namespace po = boost::program_options;

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
  auto all_writes = dariadb_bench::total_threads_count * dariadb_bench::iteration_count;
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    clock_t t1 = clock();
    auto writes_per_sec = append_count.load() / double((t1 - t0) / CLOCKS_PER_SEC);

    std::cout << "\r"
              << " caps:" << dariadb::storage::Manifest::instance()->cola_list().size()
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

  po::options_description desc("Allowed options");
  bool dont_clean = false;
  desc.add_options()("help", "produce help message")(
      "dont-clean", po::value<bool>(&dont_clean)->default_value(dont_clean),
      "enable readers threads");

  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
  } catch (std::exception &ex) {
    logger("Error: " << ex.what());
    exit(1);
  }
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 1;
  }

  dariadb::IdSet all_id_set;
  auto startTime = dariadb::timeutil::current_time();
  {
    const std::string storage_path = "testStorage";
    const size_t cap_B = 50;
    // dont_clean=true;
    if (!dont_clean && dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storage_path, "Manifest"));
    dariadb::storage::CapacitorManager::Params cap_param(storage_path, cap_B);
    cap_param.max_levels = 11;
    dariadb::storage::CapacitorManager::start(cap_param);
    auto tos = dariadb::storage::CapacitorManager::instance();

    std::thread info_thread(show_info);

    std::vector<std::thread> writers(dariadb_bench::total_threads_count);

    size_t pos = 0;
    for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
      all_id_set.insert(pos);
      std::thread t{dariadb_bench::thread_writer_rnd_stor, dariadb::Id(pos),
                    dariadb::Time(i), &append_count, tos};
      writers[pos++] = std::move(t);
    }

    pos = 0;
    for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
      std::thread t = std::move(writers[pos++]);
      t.join();
    }

    stop_info = true;
    info_thread.join();

    dariadb_bench::readBenchark(all_id_set, tos, 20, startTime,
                                dariadb::timeutil::current_time());
  }
}
