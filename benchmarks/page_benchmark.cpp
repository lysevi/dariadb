#include "bench_common.h"
#include <dariadb.h>
#include <storage/manifest.h>
#include <storage/page_manager.h>
#include <thread>
#include <utils/fs.h>
#include <utils/metrics.h>
#include <utils/thread_manager.h>

#include <boost/program_options.hpp>

namespace po = boost::program_options;

std::atomic_long append_count{0};
bool stop_info = false;

void show_info() {
  clock_t t0 = clock();

  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    clock_t t1 = clock();
    auto writes_per_sec = append_count.load() / double((t1 - t0) / CLOCKS_PER_SEC);

    std::cout << "\r"
              << " pages:" << dariadb::storage::PageManager::instance()->files_count()
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

const std::string storagePath = "page_benchmark_storage";
const size_t chunks_count = 1024;
const size_t chunks_size = 1024;

class ReadCallback : public dariadb::storage::IReaderClb {
public:
  virtual void call(const dariadb::Meas &) override { ++count; }
  size_t count;
};

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;

  po::options_description desc("Allowed options");
  bool dont_clean = false;
  bool metrics_enable = false;
  desc.add_options()("help", "produce help message")("dont-clean",
                                                     "enable readers threads")(
      "enable-metrics", po::value<bool>(&metrics_enable)->default_value(metrics_enable));

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

  if (vm.count("dont-clean")) {
    std::cout << "Dont clean storage." << std::endl;
    dont_clean = true;
  }

  if (metrics_enable) {
    std::cout << "enable metrics." << std::endl;
  }

  {
    if (dont_clean) {
      std::cout << "open run results." << std::endl;
    }
    if (!dont_clean && dariadb::utils::fs::path_exists(storagePath)) {
      std::cout << "clean last run results." << std::endl;
      dariadb::utils::fs::rm(storagePath);
    }
    if (!dont_clean) {
      dariadb::utils::fs::mkdir(storagePath);
    }
    dariadb::utils::async::ThreadManager::start(
        dariadb::utils::async::THREAD_MANAGER_COMMON_PARAMS);
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storagePath, "Manifest"));
    dariadb::storage::PageManager::start(
        dariadb::storage::PageManager::Params(storagePath, chunks_count, chunks_size));

    std::thread info_thread(show_info);

    std::vector<std::thread> writers(dariadb_bench::total_threads_count);

    size_t pos = 0;
    dariadb::IdSet all_id_set;
    for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
      auto id_from = dariadb_bench::get_id_from(pos);
      auto id_to = dariadb_bench::get_id_to(pos);
      for (size_t j = id_from; j < id_to; j++) {
        all_id_set.insert(j);
      }
      std::thread t{dariadb_bench::thread_writer_rnd_stor, dariadb::Id(pos),
                    dariadb::Time(i), &append_count,
                    dariadb::storage::PageManager::instance()};
      writers[pos++] = std::move(t);
    }

    pos = 0;
    for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
      std::thread t = std::move(writers[pos++]);
      t.join();
    }

    stop_info = true;
    info_thread.join();

    std::random_device r;
    std::default_random_engine e1(r());
    std::uniform_int_distribution<dariadb::Id> uniform_dist(
        dariadb::storage::PageManager::instance()->minTime(),
        dariadb::storage::PageManager::instance()->maxTime());

    dariadb::IdArray ids{all_id_set.begin(), all_id_set.end()};
    const size_t runs_count = 10;

    auto clb = new ReadCallback;
    dariadb::storage::ReaderClb_ptr clb_sptr{clb};
    auto start = clock();

    for (size_t i = 0; i < runs_count; i++) {
      auto time_point1 = uniform_dist(e1);
      auto time_point2 = uniform_dist(e1);
      auto from = std::min(time_point1, time_point2);
      auto to = std::max(time_point1, time_point2);
      auto qi = dariadb::storage::QueryInterval(ids, 0, from, to);
      auto link_list = dariadb::storage::PageManager::instance()->chunksByIterval(qi);

      dariadb::storage::PageManager::instance()->readLinks(qi, link_list, clb);
      assert(clb->count != 0);
      clb->count = 0;
    }

    auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "interval: " << elapsed / runs_count << std::endl;

    start = clock();

    for (size_t i = 0; i < runs_count; i++) {
      auto time_point = uniform_dist(e1);
      dariadb::storage::PageManager::instance()->valuesBeforeTimePoint(
          dariadb::storage::QueryTimePoint(ids, 0, time_point));
    }

    elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "timePoint: " << elapsed / runs_count << std::endl;

    dariadb::storage::PageManager::stop();
    dariadb::storage::Manifest::stop();
    dariadb::utils::async::ThreadManager::stop();
    if (dariadb::utils::fs::path_exists(storagePath)) {
      dariadb::utils::fs::rm(storagePath);
    }
    if (metrics_enable) {
      std::cout << "metrics:\n"
                << dariadb::utils::metrics::MetricsManager::instance()->to_string()
                << std::endl;
    }
  }
}
