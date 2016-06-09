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
#include <thread>
#include <utils/fs.h>

#include <boost/program_options.hpp>

namespace po = boost::program_options;

std::atomic_long append_count{0};
std::atomic_long reads_count{0};
bool stop_info = false;
bool stop_readers = false;

class BenchCallback : public dariadb::storage::ReaderClb {
public:
  BenchCallback() { count = 0; }
  void call(const dariadb::Meas &) { count++; }
  size_t count;
};

void show_info(dariadb::storage::Engine *storage) {
  clock_t t0 = clock();
  auto all_writes = dariadb_bench::total_threads_count * dariadb_bench::iteration_count;
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    clock_t t1 = clock();
    auto writes_per_sec = append_count.load() / double((t1 - t0) / CLOCKS_PER_SEC);
    auto reads_per_sec = reads_count.load() / double((t1 - t0) / CLOCKS_PER_SEC);
    auto queue_sizes = storage->queue_size();
    std::cout << "\r"
              << " in queue: (p:" << queue_sizes.pages_count
              << " cap:" << queue_sizes.cola_count << ")"
              << " reads: " << reads_count << " speed:" << reads_per_sec << "/sec"
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

void reader(dariadb::storage::MeasStorage_ptr ms, dariadb::IdSet all_id_set,
            dariadb::Time from, dariadb::Time to) {
  std::random_device r;
  std::default_random_engine e1(r());
  std::uniform_int_distribution<dariadb::Id> uniform_dist(from, to);
  std::shared_ptr<BenchCallback> clbk{new BenchCallback};

  while (true) {
    clbk->count = 0;
    auto time_point1 = uniform_dist(e1);
    auto time_point2 = uniform_dist(e1);
    auto f = std::min(time_point1, time_point2);
    auto t = std::max(time_point1, time_point2);

    auto qi = dariadb::storage::QueryInterval(
        dariadb::IdArray(all_id_set.begin(), all_id_set.end()), 0, f, t);
    ms->readInterval(qi)->readAll(clbk.get());

    reads_count += clbk->count;
    if (stop_readers) {
      break;
    }
  }
}

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;
  std::cout << "Performance benchmark" << std::endl;
  std::cout << "Writers count:" << dariadb_bench::total_threads_count << std::endl;
  
  const std::string storage_path = "testStorage";
  bool readers_enable = false;
  bool dont_clean = false;
  po::options_description desc("Allowed options");
  desc.add_options()
	  ("help", "produce help message")
	  ("enable-readers", po::value<bool>(&readers_enable)->default_value(readers_enable),"enable readers threads")
	  ("dont-clean", po::value<bool>(&dont_clean)->default_value(dont_clean),
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

  if (readers_enable) {
    std::cout << "Readers enable. count: "<<dariadb_bench::total_readers_count << std::endl;
  }

  {
    std::cout << "write..." << std::endl;

    const size_t chunk_per_storage = 1024 * 10;
    const size_t chunk_size = 1024;
    const size_t cap_B = 10;
    const size_t max_mem_chunks = 100;

	//dont_clean = true;
	if (!dont_clean && dariadb::utils::fs::path_exists(storage_path)) {
		dariadb::utils::fs::rm(storage_path);
	}

    dariadb::Time start_time = dariadb::timeutil::current_time();
    dariadb::storage::Capacitor::Params cap_param(cap_B, storage_path);
    cap_param.max_levels = 11;
    auto raw_ptr = new dariadb::storage::Engine(
        dariadb::storage::PageManager::Params(storage_path, chunk_per_storage,
                                              chunk_size),
        cap_param, dariadb::storage::Engine::Limits(max_mem_chunks));

    dariadb::storage::MeasStorage_ptr ms{raw_ptr};

    dariadb::IdSet all_id_set;
    append_count = 0;
    stop_info = false;

    std::thread info_thread(show_info, raw_ptr);

    std::vector<std::thread> writers(dariadb_bench::total_threads_count);
    std::vector<std::thread> readers(dariadb_bench::total_readers_count);

    size_t pos = 0;
    for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
      all_id_set.insert(pos);
      std::thread t{dariadb_bench::thread_writer_rnd_stor, dariadb::Id(pos),
                    dariadb::Time(i), &append_count, raw_ptr};
      writers[pos++] = std::move(t);
    }
    if (readers_enable) {
      pos = 0;
      for (size_t i = 1; i < dariadb_bench::total_readers_count + 1; i++) {
        std::thread t{reader, ms, all_id_set, dariadb::Time(0),
                      dariadb::timeutil::current_time()};
        readers[pos++] = std::move(t);
      }
    }

    pos = 0;
    for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
      std::thread t = std::move(writers[pos++]);
      t.join();
    }
    stop_readers = true;
    if (readers_enable) {
      pos = 0;
      for (size_t i = 1; i < dariadb_bench::total_readers_count + 1; i++) {
        std::thread t = std::move(readers[pos++]);
        t.join();
      }
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
    std::cout << "\r"
              << " in queue: (p:" << queue_sizes.pages_count
              << " cap:" << queue_sizes.cola_count << ")" << std::endl;

    dariadb_bench::readBenchark(all_id_set, ms, 10, start_time,
                                dariadb::timeutil::current_time());

    {
      std::cout << "read all..." << std::endl;
      std::shared_ptr<BenchCallback> clbk{new BenchCallback()};
      auto start = clock();
      dariadb::storage::QueryInterval qi{
          dariadb::IdArray(all_id_set.begin(), all_id_set.end()), 0, start_time,
          ms->maxTime()};
      ms->readInterval(qi)->readAll(clbk.get());

      auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
      std::cout << "readed: " << clbk->count << std::endl;
      std::cout << "time: " << elapsed << std::endl;
      auto expected =
          (dariadb_bench::iteration_count * dariadb_bench::total_threads_count);
      if (!dont_clean && clbk->count != expected) {
        std::cout << "expected: " << expected << " get:" << clbk->count << std::endl;
        throw MAKE_EXCEPTION("(clbk->count!=(iteration_count*total_threads_count))");
	  }else {
		  if (dont_clean && clbk->count < expected) {
			  std::cout << "expected: " << expected << " get:" << clbk->count << std::endl;
			  throw MAKE_EXCEPTION("(clbk->count!=(iteration_count*total_threads_count))");
		  }
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
