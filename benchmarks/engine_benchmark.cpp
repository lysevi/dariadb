#include "bench_common.h"
#include <atomic>
#include <boost/program_options.hpp>
#include <iomanip>
#include <iostream>
#include <libdariadb/engine.h>
#include <libdariadb/utils/fs.h>
using namespace dariadb;
using namespace dariadb::storage;

namespace po = boost::program_options;

std::atomic_llong append_count{0};
std::atomic_size_t reads_count{0};
Time start_time;
Time write_time = 0;
bool stop_info = false;
bool stop_readers = false;

bool readers_enable = false;
bool readonly = false;
bool readall_enabled = false;
bool dont_clean = false;
size_t read_benchmark_runs = 10;
STRATEGY strategy = STRATEGY::COMPRESSED;
size_t memory_limit = 0;
std::unique_ptr<dariadb_bench::BenchmarkSummaryInfo> summary_info;

class BenchCallback : public IReadCallback {
public:
  BenchCallback() {
    count = 0;
    is_end_called = false;
  }
  void apply(const dariadb::Meas &) override { count++; }
  void is_end() override {
    is_end_called = true;
    IReadCallback::is_end();
  }
  std::atomic<size_t> count;
  bool is_end_called;
};

struct JoinCallback : public Join::Callback {
  JoinCallback() { calls = size_t(0); }
  void apply(const MeasArray &row) override { calls++; }

  size_t calls;
};

void parse_cmdline(int argc, char *argv[]) {
  po::options_description desc("Allowed options");
  auto aos = desc.add_options();
  aos("help", "produce help message");
  aos("readonly", "readonly mode");
  aos("readall", "read all benchmark enable.");
  aos("dont-clean", "dont clean storage path before start.");
  aos("enable-readers",
      po::value<bool>(&readers_enable)->default_value(readers_enable),
      "enable readers threads");
  aos("read-benchmark-runs",
      po::value<size_t>(&read_benchmark_runs)
          ->default_value(read_benchmark_runs));
  aos("strategy", po::value<STRATEGY>(&strategy)->default_value(strategy),
      "Write strategy");
  aos("memory-limit",
      po::value<size_t>(&memory_limit)->default_value(memory_limit),
      "allocation area limit  in megabytes when strategy=MEMORY");

  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
  } catch (std::exception &ex) {
    logger("Error: ", ex.what());
    exit(1);
  }
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    std::exit(0);
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
}

void show_info(Engine *storage) {
  const auto OUT_SEP = ' ';
  clock_t t0 = clock();
  long long w0 = append_count.load();
  long long r0 = reads_count.load();
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    long long w1 = append_count.load();
    long long r1 = reads_count.load();

    clock_t t1 = clock();
    auto step_time = double(double(t1 - t0) / (double)CLOCKS_PER_SEC);

    auto writes_per_sec = (w1 - w0) / step_time;
    auto reads_per_sec = (r1 - r0) / step_time;
    auto queue_sizes = storage->description();

    std::stringstream time_ss;
    time_ss << timeutil::to_string(write_time);

    std::stringstream stor_ss;
    stor_ss << "(p:" << queue_sizes.pages_count
            << " w:" << queue_sizes.wal_count
            << " T:" << queue_sizes.active_works;
    if ((strategy == STRATEGY::MEMORY) || (strategy == STRATEGY::CACHE)) {
      stor_ss << " am:" << queue_sizes.memstorage.allocator_capacity
              << " a:" << queue_sizes.memstorage.allocated;
    }
    stor_ss << ")";

    std::stringstream read_speed_ss;
    read_speed_ss << reads_per_sec << "/s";

    std::stringstream write_speed_ss;
    write_speed_ss << writes_per_sec << "/s :";

    std::stringstream persent_ss;
    persent_ss << (int64_t(100) * append_count) / dariadb_bench::all_writes
               << '%';

    std::stringstream drop_ss;
    drop_ss << "[a:" << queue_sizes.dropper.wal << "]";
    std::stringstream ss;

    ss // << "\r"
        << " time: " << std::setw(20) << std::setfill(OUT_SEP) << time_ss.str()
        << " storage:" << stor_ss.str() << drop_ss.str()
        << " rd: " << reads_count << " s:" << read_speed_ss.str()
        << " wr: " << append_count << " s:" << write_speed_ss.str()
        << persent_ss.str();
    dariadb::logger(ss.str());
    w0 = w1;
    t0 = t1;
    std::cout.flush();
    if (stop_info) {
      std::cout.flush();
      break;
    }
  }
  std::cout << "\n";
}

void show_drop_info(Engine *storage) {
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto queue_sizes = storage->description();

    dariadb::logger_info(" storage: (p:", queue_sizes.pages_count,
                         " w:", queue_sizes.wal_count,
                         " T:", queue_sizes.active_works, ")",
                         "[w:", queue_sizes.dropper.wal, "]");

    if (stop_info) {
      std::cout.flush();
      break;
    }
  }
  std::cout << "\n";
}

void reader(IMeasStorage_ptr ms, IdSet all_id_set, Time from, Time to) {
  std::random_device r;
  std::default_random_engine e1(r());
  std::uniform_int_distribution<dariadb::Id> uniform_dist(from, to);
  std::shared_ptr<BenchCallback> clbk{new BenchCallback};

  while (true) {
    clbk->count = 0;
    auto f = from;
    auto t = write_time;

    auto qi = dariadb::QueryInterval(
        dariadb::IdArray(all_id_set.begin(), all_id_set.end()), 0, f, t);
    ms->foreach (qi, clbk.get());

    reads_count += clbk->count;
    if (stop_readers) {
      break;
    }
  }
}

void rw_benchmark(IMeasStorage_ptr &ms, Engine *raw_ptr, Time start_time,
                  IdSet &all_id_set) {

  std::thread info_thread(show_info, raw_ptr);

  std::vector<std::thread> writers(dariadb_bench::total_threads_count);
  std::vector<std::thread> readers(dariadb_bench::total_readers_count);

  size_t pos = 0;

  for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
    auto id_from = dariadb_bench::get_id_from(pos);
    auto id_to = dariadb_bench::get_id_to(pos);
    for (size_t j = id_from; j < id_to; j++) {
      all_id_set.insert(j);
    }
    if (!readonly) {
      std::thread t{dariadb_bench::thread_writer_rnd_stor,
                    Id(pos),
                    &append_count,
                    raw_ptr,
                    start_time,
                    &write_time};
      writers[pos] = std::move(t);
    }
    pos++;
  }
  if (readers_enable) {
    pos = 0;
    for (size_t i = 1; i < dariadb_bench::total_readers_count + 1; i++) {
      std::thread t{reader, ms, all_id_set, start_time,
                    timeutil::current_time()};
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

  if (readers_enable) {
    pos = 0;
    for (size_t i = 1; i < dariadb_bench::total_readers_count + 1; i++) {
      std::thread t = std::move(readers[pos++]);
      t.join();
    }
  }

  stop_info = true;
  info_thread.join();
}

void read_all_bench(Engine *ms, Time start_time, Time max_time,
                    IdSet &all_id_set) {

  if (readonly) {
    start_time = Time(0);
  }

  std::shared_ptr<BenchCallback> clbk{new BenchCallback()};

  QueryInterval qi{IdArray(all_id_set.begin(), all_id_set.end()), 0, start_time,
                   max_time};

  std::cout << "==> foreach all..." << std::endl;

  auto start = clock();

  ms->foreach (qi, clbk.get());
  clbk->wait();

  auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
  std::cout << "readed: " << clbk->count << std::endl;
  std::cout << "time: " << elapsed << std::endl;
  summary_info->foreach_read_all_time = elapsed;

  std::cout << "==> join all..." << std::endl;

  auto join_callback = std::make_unique<JoinCallback>();

  start = clock();

  ms->join(std::list<QueryInterval>{qi}, join_callback.get());

  elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
  std::cout << "table size: " << join_callback->calls << std::endl;
  std::cout << "time: " << elapsed << std::endl;
  summary_info->join_all_time = elapsed;
  summary_info->join_table_size = join_callback->calls;

  if (readall_enabled) {
    std::cout << "==> read all..." << std::endl;

    start = clock();

    auto readed = ms->readInterval(qi);

    elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
    std::cout << "readed: " << readed.size() << std::endl;
    std::cout << "time: " << elapsed << std::endl;
    summary_info->read_all_time = elapsed;
    std::map<Id, MeasList> _dict;
    for (auto &v : readed) {
      _dict[v.id].push_back(v);
    }

    if (readed.size() != dariadb_bench::all_writes) {
      std::cout << "expected: " << dariadb_bench::all_writes
                << " get:" << clbk->count << std::endl;
      std::cout << " all_writes: " << dariadb_bench::all_writes;
      for (auto &kv : _dict) {
        std::cout << " " << kv.first << " -> " << kv.second.size() << std::endl;
      }
      throw MAKE_EXCEPTION(
          "(clbk->count!=(iteration_count*total_threads_count))");
    }
  }
}

void check_engine_state(dariadb::storage::Settings_ptr settings,
                        Engine *raw_ptr) {
  std::cout << "==> Check storage state(" << strategy << ")... " << std::flush;

  auto files = raw_ptr->description();
  switch (strategy) {
  case dariadb::STRATEGY::WAL:
    if (files.pages_count != 0) {
      THROW_EXCEPTION("WAL error: (p:", files.pages_count,
                      " a:", files.wal_count, " T:", files.active_works, ")");
    }
    break;
  case dariadb::STRATEGY::COMPRESSED:
    if (files.wal_count >= 1 && files.pages_count == 0) {
      THROW_EXCEPTION("COMPRESSED error: (p:", files.pages_count,
                      " a:", files.wal_count, " T:", files.active_works, ")");
    }
    break;
  case dariadb::STRATEGY::MEMORY:
    if (files.wal_count != 0 && files.pages_count == 0) {
      THROW_EXCEPTION("MEMORY error: (p:", files.pages_count,
                      " a:", files.wal_count, " T:", files.active_works, ")");
    }
    break;
  case dariadb::STRATEGY::CACHE:
    break;
  default:
    THROW_EXCEPTION("unknow strategy: ", strategy);
  }
  std::cout << "OK" << std::endl;
}

int main(int argc, char *argv[]) {
  dariadb::utils::ILogger_ptr log_ptr{new dariadb_bench::BenchmarkLogger};
  dariadb::utils::LogManager::start(log_ptr);

  std::cout << "Performance benchmark" << std::endl;
  std::cout << "Writers count:" << dariadb_bench::total_threads_count
            << std::endl;

  const std::string storage_path = "engine_benchmark_storage";

  parse_cmdline(argc, argv);

  if (readers_enable) {
    std::cout << "Readers enable. count: " << dariadb_bench::total_readers_count
              << std::endl;
  }
  summary_info =
      std::make_unique<dariadb_bench::BenchmarkSummaryInfo>(strategy);

  {
    std::cout << "Write..." << std::endl;

    bool is_exists = false;
    if (dariadb::utils::fs::path_exists(storage_path)) {
      if (!dont_clean) {
        if (!readonly) {
          std::cout << " remove " << storage_path << std::endl;
          dariadb::utils::fs::rm(storage_path);
        }
      } else {
        is_exists = true;
      }
    }

    auto settings = dariadb::storage::Settings::create(storage_path);
    settings->strategy.setValue(strategy);
    settings->save();

    if ((strategy == STRATEGY::MEMORY || strategy == STRATEGY::CACHE) &&
        memory_limit != 0) {
      std::cout << "memory limit: " << memory_limit << std::endl;
      settings->memory_limit.setValue(memory_limit * 1024 * 1024);
    } else {
      if (strategy == STRATEGY::MEMORY) {
        memory_limit = 350;
        std::cout << "default memory limit: " << memory_limit << std::endl;
        settings->memory_limit.setValue(memory_limit * 1024 * 1024);
      }
    }

    utils::LogManager::start(log_ptr);

    auto raw_ptr = new Engine(settings);

    if (is_exists) {
      raw_ptr->fsck();
    }
    IMeasStorage_ptr ms{raw_ptr};

    dariadb::IdSet all_id_set;
    append_count = 0;
    stop_info = false;
    auto writers_start = clock();

    start_time = dariadb::timeutil::current_time();
    rw_benchmark(ms, raw_ptr, start_time, all_id_set);

    auto writers_elapsed = (((float)clock() - writers_start) / CLOCKS_PER_SEC);
    stop_readers = true;

    std::cout << "total id:" << all_id_set.size() << std::endl;

    std::cout << "write time: " << writers_elapsed << std::endl;
    std::cout << "total speed: " << append_count / writers_elapsed << "/s"
              << std::endl;
    if (strategy != STRATEGY::MEMORY && strategy != STRATEGY::CACHE) {
      std::cout << "==> full flush..." << std::endl;
      stop_info = false;
      std::thread flush_info_thread(show_drop_info, raw_ptr);

      auto start = clock();
      raw_ptr->flush();

      { raw_ptr->wait_all_asyncs(); }
      auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
      stop_info = true;
      flush_info_thread.join();
      std::cout << "flush time: " << elapsed << std::endl;
    }

    check_engine_state(settings, raw_ptr);

    if (!readonly) {
      if (strategy != dariadb::STRATEGY::MEMORY &&
          strategy != STRATEGY::CACHE) {
        size_t ccount = size_t(raw_ptr->description().wal_count);
        std::cout << "==> drop part wals to " << ccount << "..." << std::endl;
        stop_info = false;
        std::thread flush_info_thread(show_drop_info, raw_ptr);

        auto start = clock();
        raw_ptr->drop_part_wals(ccount);
        raw_ptr->flush();
        raw_ptr->wait_all_asyncs();
        auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
        stop_info = true;
        flush_info_thread.join();
        std::cout << "drop time: " << elapsed << std::endl;
      }
      {
        auto pages_before = raw_ptr->description().pages_count;
        if (pages_before != 0) {
          std::cout << "==> pages before repack " << pages_before << "..."
                    << std::endl;
          auto start = clock();
          raw_ptr->repack();
          auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
          auto pages_after = raw_ptr->description().pages_count;
          std::cout << "==> pages after repack " << pages_after << "..."
                    << std::endl;
          std::cout << "repack time: " << elapsed << std::endl;
          summary_info->page_repacked = pages_before - pages_after - 1;
          summary_info->page_repack_time = elapsed;
          if (strategy != STRATEGY::MEMORY && strategy != STRATEGY::CACHE &&
              pages_before <= pages_after) {
            THROW_EXCEPTION("pages_before <= pages_after");
          }
        }
      }
    }

    auto queue_sizes = raw_ptr->description();
    std::cout << "\r"
              << " storage: (p:" << queue_sizes.pages_count
              << " a:" << queue_sizes.wal_count << ")" << std::endl;

    std::cout << "Active threads: "
              << utils::async::ThreadManager::instance()->active_works()
              << std::endl;

    summary_info->writed = append_count;
    summary_info->write_speed = append_count / writers_elapsed;
    dariadb_bench::readBenchmark(summary_info.get(), all_id_set, ms.get(),
                                 read_benchmark_runs);

    auto max_time = ms->maxTime();
    std::cout << "==> interval end time: " << timeutil::to_string(max_time)
              << std::endl;

    read_all_bench(raw_ptr, start_time, max_time, all_id_set);
    std::cout << "writed: " << append_count << std::endl;
    std::cout << "stoping storage...\n";
    ms = nullptr;
    settings = nullptr;
    auto blog = dynamic_cast<dariadb_bench::BenchmarkLogger *>(log_ptr.get());
    if (blog->_calls.load() == 0) {
      throw std::logic_error("log_ptr->_calls.load()==0");
    }

    summary_info->print();
  }

  if (!(dont_clean || readonly) && (utils::fs::path_exists(storage_path))) {
    std::cout << "cleaning...\n";
    utils::fs::rm(storage_path);
  }
}
