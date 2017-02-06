#ifdef MSVC
#define _SCL_SECURE_NO_WARNINGS
#endif
#include <chrono>
#include <cmath>
#include <iostream>
#include <map>
#include <numeric>
#include <thread>
#include <vector>

#include "bench_common.h"

#include <libdariadb/meas.h>
#include <libdariadb/storage/memstorage/memstorage.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/fs.h>

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/program_options.hpp>

namespace po = boost::program_options;

using namespace dariadb;
using namespace dariadb::storage;

std::atomic_llong append_count{0};
dariadb::Time write_time = 0;
bool stop_info = false;
dariadb::storage::MemStorage_ptr mstore;
size_t memory_limit = 0;

void show_info() {
  clock_t t0 = clock();

  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    clock_t t1 = clock();
    auto writes_per_sec = append_count.load() / double((t1 - t0) / CLOCKS_PER_SEC);
    auto dscr = mstore->description();
    std::cout << "\r"
              << " writes: " << append_count << " speed: " << writes_per_sec << "/sec"
              << " [m:" << dscr.allocator_capacity << ", a:" << dscr.allocated << "]"
              << " progress:" << (int64_t(100) * append_count) / dariadb_bench::all_writes
              << "%                ";
    std::cout.flush();
    if (stop_info) {
      std::cout.flush();
      break;
    }
  }
  std::cout << "\n";
}

int main(int argc, char **argv) {
  po::options_description desc("Allowed options");
  auto aos = desc.add_options()("help", "produce help message");
  aos("memory-limit", po::value<size_t>(&memory_limit)->default_value(memory_limit),
      "allocation area limit  in megabytes when strategy=MEMORY");

  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
  } catch (std::exception &ex) {
    dariadb::logger("Error: ", ex.what());
    exit(1);
  }
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 1;
  }

  dariadb::IdSet all_id_set;
  {
    auto storage_path = "testMemoryStorage";
    if (dariadb::utils::fs::path_exists(storage_path)) {
      std::cout << " remove " << storage_path << std::endl;
      dariadb::utils::fs::rm(storage_path);
    }
    auto settings = dariadb::storage::Settings::create(storage_path);
    settings->memory_limit.setValue(500 * 1024 * 1024);
    settings->chunk_size.setValue(1024);
    auto _engine_env = dariadb::storage::EngineEnvironment::create();
    _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
                             settings.get());
    dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

    if (memory_limit != 0) {
      std::cout << "memory limit: " << memory_limit << std::endl;
      settings->memory_limit.setValue(memory_limit * 1024 * 1024);
    }
    mstore = dariadb::storage::MemStorage::create(_engine_env, size_t(0));

    std::thread info_thread(show_info);

    std::vector<std::thread> writers(dariadb_bench::total_threads_count);

    size_t pos = 0;
    for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
      all_id_set.insert(pos);
      std::thread t{
          dariadb_bench::thread_writer_rnd_stor, dariadb::Id(pos), &append_count, mstore.get(),
          dariadb::timeutil::current_time(),     &write_time};
      writers[pos++] = std::move(t);
    }

    pos = 0;
    for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
      std::thread t = std::move(writers[pos++]);
      t.join();
    }

    stop_info = true;
    info_thread.join();
	dariadb_bench::BenchmarkSummaryInfo summary_info(dariadb::STRATEGY::MEMORY);
    dariadb_bench::readBenchmark(&summary_info, all_id_set, mstore.get(), 10, false, false);

    mstore=nullptr;
    dariadb::utils::async::ThreadManager::stop();
  }
}
