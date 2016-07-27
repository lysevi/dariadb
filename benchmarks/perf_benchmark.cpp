#include "bench_common.h"
#include <dariadb.h>
#include <storage/capacitor.h>
#include <utils/fs.h>
#include <utils/metrics.h>
#include <utils/thread_manager.h>

#include <boost/program_options.hpp>

namespace po = boost::program_options;

std::atomic_llong append_count{0};
std::atomic_size_t reads_count{0};
bool stop_info = false;
bool stop_readers = false;

class BenchCallback : public dariadb::storage::IReaderClb {
public:
  BenchCallback() { count = 0; }
  void call(const dariadb::Meas &v) { count++; }
  std::atomic<size_t> count;
};

void show_info(dariadb::storage::Engine *storage) {
  clock_t t0 = clock();
  long long w0=append_count.load();
  long long r0=reads_count.load();
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    long long w1=append_count.load();
    long long r1=reads_count.load();

    clock_t t1 = clock();
    auto step_time=double(double(t1 - t0) / (double)CLOCKS_PER_SEC);

    auto writes_per_sec = (w1-w0) /step_time;
    auto reads_per_sec = (r1-r0) / step_time;
    auto queue_sizes = storage->queue_size();

    std::cout << "\r"
              << " in queue: (p:" << queue_sizes.pages_count
              << " cap:" << queue_sizes.cola_count << " a:" << queue_sizes.aofs_count
              << " T:" << queue_sizes.active_works << ") reads: " << reads_count
              << " speed:" << reads_per_sec << "/sec"
              << " writes: " << append_count << " speed: " << writes_per_sec
              << "/sec progress:"
              << (int64_t(100) * append_count) / dariadb_bench::all_writes
              << "%                ";
    w0=w1;
    t0=t1;
    std::cout.flush();
    if (stop_info) {
      std::cout.flush();
      break;
    }
  }
  std::cout << "\n";
}

void show_drop_info(dariadb::storage::Engine *storage) {
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto queue_sizes = storage->queue_size();
    std::cout << "\r"
              << " in queue: (p:" << queue_sizes.pages_count
              << " cap:" << queue_sizes.cola_count << " a:" << queue_sizes.aofs_count
              << " T:" << queue_sizes.active_works << ")          ";
    std::cout.flush();
    if (stop_info) {
      std::cout.flush();
      break;
    }
  }
  std::cout << "\n";
}

void reader(dariadb::storage::IMeasStorage_ptr ms, dariadb::IdSet all_id_set,
            dariadb::Time from, dariadb::Time to) {
  std::random_device r;
  std::default_random_engine e1(r());
  std::uniform_int_distribution<dariadb::Id> uniform_dist(from, to);
  std::shared_ptr<BenchCallback> clbk{new BenchCallback};

  while (true) {
    clbk->count = 0;
    // auto time_point1 = uniform_dist(e1);
    auto f = from;
    auto t = dariadb::timeutil::current_time();

    auto qi = dariadb::storage::QueryInterval(
        dariadb::IdArray(all_id_set.begin(), all_id_set.end()), 0, f, t);
    ms->foreach (qi, clbk.get());

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

  const std::string storage_path = "perf_benchmark_storage";
  bool readers_enable = false;
  bool metrics_enable = false;
  bool readonly = false;
  bool readall_enabled = false;
  bool dont_clean = false;
  // bool full_flush = false;
  po::options_description desc("Allowed options");
  desc.add_options()("help", "produce help message")("readonly", "readonly mode")(
      "readall", "read all benchmark enable.")
      //("full-flush", "wait end of all async tasks.")
      ("dont-clean", "dont clean storage path before start.")(
          "enable-readers",
          po::value<bool>(&readers_enable)->default_value(readers_enable),
          "enable readers threads")(
          "enable-metrics",
          po::value<bool>(&metrics_enable)->default_value(metrics_enable));

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

  if (metrics_enable) {
    std::cout << "Enable metrics." << std::endl;
  }

  if (vm.count("readonly")) {
    std::cout << "Readonly mode." << std::endl;
    readonly = true;
  }

  if (vm.count("readall")) {
    std::cout << "Read all benchmark enabled." << std::endl;
    readall_enabled = true;
  }

  if (vm.count("dont-clean")) {
    std::cout << "Dont clean storage." << std::endl;
    dont_clean = true;
  }

  /*if (vm.count("full-flush")) {
          std::cout << "full-flush." << std::endl;
          full_flush = true;
  }*/

  if (readers_enable) {
    std::cout << "Readers enable. count: " << dariadb_bench::total_readers_count
              << std::endl;
  }

  {
    std::cout << "Write..." << std::endl;

    const size_t chunk_size = 512;
    const size_t cap_B = 50;

    // dont_clean = true;
    bool is_exists=false;
    if (!dont_clean && dariadb::utils::fs::path_exists(storage_path)) {

      if (!readonly) {
        std::cout << " remove " << storage_path << std::endl;
        dariadb::utils::fs::rm(storage_path);
      }else{
          is_exists=true;
      }
    }

    dariadb::Time start_time = dariadb::timeutil::current_time();
    std::cout << " start time: " << dariadb::timeutil::to_string(start_time) << std::endl;

    dariadb::storage::PageManager::Params page_param(storage_path, chunk_size);
    
	dariadb::storage::CapacitorManager::Params cap_param(storage_path, cap_B);
	cap_param.store_period = 0;// 1000 * 60 * 60;
	cap_param.max_levels = 11;
	cap_param.max_closed_caps = 0; // 5;
	
	dariadb::storage::AOFManager::Params aof_param(storage_path, 0);
	aof_param.buffer_size = 1000;
	aof_param.max_size = cap_param.measurements_count();
    auto raw_ptr = new dariadb::storage::Engine(aof_param, page_param, cap_param);

    if (is_exists) {
        raw_ptr->fsck();
    }
    dariadb::storage::IMeasStorage_ptr ms{raw_ptr};

    dariadb::IdSet all_id_set;
    append_count = 0;
    stop_info = false;

    std::thread info_thread(show_info, raw_ptr);

    std::vector<std::thread> writers(dariadb_bench::total_threads_count);
    std::vector<std::thread> readers(dariadb_bench::total_readers_count);

    size_t pos = 0;
	auto writers_start = clock();
    for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
      auto id_from = dariadb_bench::get_id_from(pos);
      auto id_to = dariadb_bench::get_id_to(pos);
      for (size_t j = id_from; j < id_to; j++) {
        all_id_set.insert(j);
      }
      if (!readonly) {
        std::thread t{dariadb_bench::thread_writer_rnd_stor, dariadb::Id(pos),
                      dariadb::Time(i), &append_count, raw_ptr};
        writers[pos] = std::move(t);
      }
      pos++;
    }
    if (readers_enable) {
      pos = 0;
      for (size_t i = 1; i < dariadb_bench::total_readers_count + 1; i++) {
        std::thread t{reader, ms, all_id_set, start_time,
                      dariadb::timeutil::current_time()};
        readers[pos++] = std::move(t);
      }
    }
	
	

    if (!readonly) {
      pos = 0;
      for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
        std::thread t = std::move(writers[pos++]);
        t.join();
      }
    }
	auto writers_elapsed = (((float)clock() - writers_start) / CLOCKS_PER_SEC);
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
    std::cout << "total id:" << all_id_set.size() << std::endl;

    std::cout << "write time: " << writers_elapsed <<std::endl;
    std::cout <<"total speed: "<<append_count /writers_elapsed<<"/s" << std::endl;
    {
      std::cout << "==> full flush..." << std::endl;
      stop_info = false;
      std::thread flush_info_thread(show_drop_info, raw_ptr);

      auto start = clock();
      raw_ptr->flush();
      // if (full_flush)
      { raw_ptr->wait_all_asyncs(); }
      auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
      stop_info = true;
      flush_info_thread.join();
      std::cout << "flush time: " << elapsed << std::endl;
    }

    if (!readonly) {
      size_t ccount = size_t(raw_ptr->queue_size().cola_count*0.5);
      std::cout << "==> drop part caps to " << ccount << "..." << std::endl;
      stop_info = false;
      std::thread flush_info_thread(show_drop_info, raw_ptr);

      auto start = clock();
      raw_ptr->drop_part_caps(ccount);
      raw_ptr->flush();
      raw_ptr->wait_all_asyncs();
      auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
      stop_info = true;
      flush_info_thread.join();
      std::cout << "drop time: " << elapsed << std::endl;
    }

    auto queue_sizes = raw_ptr->queue_size();
    std::cout << "\r"
              << " in queue: (p:" << queue_sizes.pages_count
              << " cap:" << queue_sizes.cola_count << " a:" << queue_sizes.aofs_count
              << ")" << std::endl;

    std::cout << "Active threads: "
              << dariadb::utils::async::ThreadManager::instance()->active_works()
              << std::endl;

    dariadb_bench::readBenchark(all_id_set, ms.get(), 100, start_time,
                                dariadb::timeutil::current_time());

	auto max_time = ms->maxTime();
	std::cout << "==> interval end time: " << dariadb::timeutil::to_string(max_time)
		<< std::endl;

    if (readall_enabled) {
      if (readonly) {
        start_time = dariadb::Time(0);
      }

      std::shared_ptr<BenchCallback> clbk{new BenchCallback()};
      

      dariadb::storage::QueryInterval qi{
          dariadb::IdArray(all_id_set.begin(), all_id_set.end()), 0, start_time,
          max_time};

      std::cout << "==> foreach all..." << std::endl;

      auto start = clock();

      ms->foreach (qi, clbk.get());

      auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
      std::cout << "readed: " << clbk->count << std::endl;
      std::cout << "time: " << elapsed << std::endl;

      std::cout << "==> read all..." << std::endl;

      start = clock();

      auto readed = ms->readInterval(qi);

      elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
      std::cout << "readed: " << readed.size() << std::endl;
      std::cout << "time: " << elapsed << std::endl;

      auto expected = (dariadb_bench::write_per_id_count *
                       dariadb_bench::total_threads_count * dariadb_bench::id_per_thread);

      std::map<dariadb::Id, dariadb::Meas::MeasList> _dict;
      for (auto &v : readed) {
        _dict[v.id].push_back(v);
      }

      if (readed.size() != expected) {
        std::cout << "expected: " << expected << " get:" << clbk->count << std::endl;
		std::cout << " all_writesL " << dariadb_bench::all_writes;
        for (auto &kv : _dict) {
          std::cout << " " << kv.first << " -> " << kv.second.size() << std::endl;
        }
        throw MAKE_EXCEPTION("(clbk->count!=(iteration_count*total_threads_count))");
      }
    }
    std::cout << "stoping storage...\n";
    ms = nullptr;
  }

  if (!(dont_clean || readonly) && (dariadb::utils::fs::path_exists(storage_path))) {
    std::cout << "cleaning...\n";
    dariadb::utils::fs::rm(storage_path);
  }

  if (metrics_enable) {
    std::cout << "metrics:\n"
              << dariadb::utils::metrics::MetricsManager::instance()->to_string()
              << std::endl;
  }
}
