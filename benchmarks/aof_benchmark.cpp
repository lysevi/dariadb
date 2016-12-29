#include "bench_common.h"
#include <libdariadb/storage/aof/aof_manager.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/metrics.h>

#include <boost/program_options.hpp>

namespace po = boost::program_options;

std::atomic_llong append_count{0};
dariadb::Time write_time = 0;
bool stop_info = false;
dariadb::storage::AOFManager_ptr aof_manager;

void show_info() {
  clock_t t0 = clock();

  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    clock_t t1 = clock();
    auto writes_per_sec = append_count.load() / double((t1 - t0) / CLOCKS_PER_SEC);

    std::cout << "\r"
              << " wal: " << aof_manager->filesCount()
              << " writes: " << append_count << " speed: " << writes_per_sec
              << "/sec progress:"
              << (int64_t(100) * append_count) / dariadb_bench::all_writes
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
  dariadb::utils::ILogger_ptr log_ptr{new dariadb_bench::BenchmarkLogger};
  dariadb::utils::LogManager::start(log_ptr);

  po::options_description desc("Allowed options");
  bool dont_clean = false;
  bool metrics_enable = false;
  auto aos = desc.add_options();
  aos("help", "produce help message");
  aos("dont-clean", "dont clean storage path before start.");
  aos("enable-metrics", po::value<bool>(&metrics_enable)->default_value(metrics_enable));

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

  if (metrics_enable) {
    std::cout << "enable metrics." << std::endl;
  }

  if (vm.count("dont-clean")) {
    std::cout << "Dont clean storage." << std::endl;
    dont_clean = true;
  }

  dariadb::IdSet all_id_set;
  {
    const std::string storage_path = "aof_benchmark_storage";

    if (!dont_clean && dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    dariadb::utils::fs::mkdir(storage_path);
	
	auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
	
	auto manifest = dariadb::storage::Manifest_ptr{ new dariadb::storage::Manifest{ settings } };
	
	auto _engine_env = dariadb::storage::EngineEnvironment_ptr{ new dariadb::storage::EngineEnvironment() };
	_engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS, settings.get());
	_engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::MANIFEST, manifest.get());

    dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

    aof_manager = dariadb::storage::AOFManager_ptr{ new dariadb::storage::AOFManager(_engine_env) };

	auto aof = aof_manager.get();
    std::thread info_thread(show_info);

    std::vector<std::thread> writers(dariadb_bench::total_threads_count);

    size_t pos = 0;
    for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
      all_id_set.insert(pos);
      std::thread t{
          dariadb_bench::thread_writer_rnd_stor, dariadb::Id(pos), &append_count, aof,
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

    dariadb_bench::readBenchark(all_id_set, aof, 10);

	manifest = nullptr;
    dariadb::utils::async::ThreadManager::stop();

    if (metrics_enable) {
      std::cout << "metrics:\n"
                << dariadb::utils::metrics::MetricsManager::instance()->to_string()
                << std::endl;
    }
  }
}
