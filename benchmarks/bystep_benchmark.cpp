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

#include <libdariadb/storage/bystep/bystep_storage.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/utils/fs.h>

#include <boost/program_options.hpp>

namespace po = boost::program_options;

using namespace dariadb;
using namespace dariadb::storage;

std::atomic_llong append_count{0};
dariadb::Time write_time = 0;
bool stop_info = false;
dariadb::storage::ByStepStorage_ptr bs_storage;
dariadb::storage::STEP_KIND step_kind = dariadb::storage::STEP_KIND::SECOND;

void show_info() {
  clock_t t0 = clock();

  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    clock_t t1 = clock();
    auto writes_per_sec = append_count.load() / double((t1 - t0) / CLOCKS_PER_SEC);
    auto d = bs_storage->description();
    std::stringstream ss;
    ss << "(q:" << d.in_queue << ")"
       << " writes: " << append_count << " speed: " << writes_per_sec << "/sec"
       << " progress:" << (int64_t(100) * append_count) / dariadb_bench::all_writes
       << "%";
    dariadb::logger_info(ss.str());
    if (stop_info) {
      break;
    }
  }
}

int main(int argc, char **argv) {
  po::options_description desc("Allowed options");
  bool metrics_enable = false;
  auto aos = desc.add_options()("help", "produce help message");
  aos("step", po::value<STEP_KIND>(&step_kind)->default_value(step_kind), "step kind");

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

  dariadb::IdSet all_id_set;
  {
    auto storage_path = "testByStepStorage";
    if (dariadb::utils::fs::path_exists(storage_path)) {
      std::cout << " remove " << storage_path << std::endl;
      dariadb::utils::fs::rm(storage_path);
    }
    auto settings = Settings_ptr{new dariadb::storage::Settings(storage_path)};
    auto _engine_env = EngineEnvironment_ptr{new EngineEnvironment()};
    _engine_env->addResource(EngineEnvironment::Resource::SETTINGS, settings.get());
    dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

    bs_storage = ByStepStorage_ptr{new ByStepStorage{_engine_env}};

    std::thread info_thread(show_info);

    std::vector<std::thread> writers(dariadb_bench::total_threads_count);

    dariadb::storage::Id2Step steps;
    for (size_t i = 0; i < dariadb_bench::id_count; ++i) {
      steps[i] = step_kind;
    }
    bs_storage->set_steps(steps);
    size_t pos = 0;
    for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
      all_id_set.insert(pos);
      std::thread t{dariadb_bench::thread_writer_rnd_stor,
                    dariadb::Id(pos),
                    &append_count,
                    bs_storage.get(),
                    dariadb::timeutil::current_time(),
                    &write_time};
      writers[pos++] = std::move(t);
    }

    pos = 0;
    for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
      std::thread t = std::move(writers[pos++]);
      t.join();
    }

    stop_info = true;
    info_thread.join();
	
	bs_storage->flush();

    dariadb_bench::readBenchark(all_id_set, bs_storage.get(), 10, false, false);
	
	bs_storage = nullptr;
    dariadb::utils::async::ThreadManager::stop();

    if (metrics_enable) {
      std::cout << "metrics:\n"
                << dariadb::utils::metrics::MetricsManager::instance()->to_string()
                << std::endl;
    }
  }
}
