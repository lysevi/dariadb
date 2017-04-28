#include "bench_common.h"
#include <libdariadb/engines/engine.h>
#include <libdariadb/engines/shard.h>
#include <libdariadb/utils/fs.h>
#include <boost/program_options.hpp>
#include <atomic>
#include <iomanip>
#include <iostream>
using namespace dariadb;
using namespace dariadb::storage;

typedef std::list<dariadb::Meas> MeasesList;

namespace po = boost::program_options;

const std::string storage_path = "engine_benchmark_storage";

std::atomic_llong append_count{0};
std::atomic_size_t reads_count{0};
Time start_time;
Time write_time = 0;
bool stop_info = false;
bool stop_readers = false;

bool readers_enable = false;
bool readonly = false;
bool write_only = false;
bool readall_enabled = false;
bool dont_clean = false;
bool use_shard = false;
bool dont_repack = false;
bool memory_only = false;
size_t read_benchmark_runs = 10;
STRATEGY strategy = STRATEGY::COMPRESSED;
size_t memory_limit = 0;
std::unique_ptr<dariadb_bench::BenchmarkSummaryInfo> summary_info;

dariadb_bench::BenchmarkParams benchmark_params;

class CompactionBenchmark : public dariadb::ICompactionController {
public:
  CompactionBenchmark(dariadb::Id id, dariadb::Time eraseThan, dariadb::Time from,
                      dariadb::Time to)
      : dariadb::ICompactionController(id, eraseThan, from, to) {}

  void compact(dariadb::MeasArray &values, std::vector<int> &filter) override {
    for (size_t i = 0; i < values.size(); ++i) {
      auto v = values[i];
      if ((v.id % 2) == 1) {
        filter[i] = i % 2 ? 1 : 0;
      } else {
        filter[i] = 0;
      }
    }
  }
};

class BenchCallback : public IReadCallback {
public:
  BenchCallback() {
    count = 0;
    is_end_called = false;
  }
  void apply(const dariadb::Meas &m) override { count++; }
  void is_end() override {
    is_end_called = true;
    IReadCallback::is_end();
  }
  std::atomic<size_t> count;
  bool is_end_called;
};

void parse_cmdline(int argc, char *argv[]) {
  po::options_description desc("Allowed options");
  auto aos = desc.add_options();
  aos("help", "produce help message");
  aos("readonly", "readonly mode");
  aos("writeonly", "writeonly mode");
  aos("readall", "read all benchmark enable.");
  aos("dont-clean", "dont clean storage path before start.");
  aos("enable-readers", "enable readers threads");
  aos("read-benchmark-runs",
      po::value<size_t>(&read_benchmark_runs)->default_value(read_benchmark_runs));

  aos("strategy", po::value<STRATEGY>(&strategy)->default_value(strategy),
      "Write strategy");
  aos("memory-limit", po::value<size_t>(&memory_limit)->default_value(memory_limit),
      "allocation area limit  in megabytes when strategy=MEMORY");
  aos("use-shard", "shard some id per shards");
  aos("dont-repack", "do not run repack and compact");
  aos("memory-only", "dont use  the disk");

  po::options_description writers("Writers params");
  auto aos_writers = writers.add_options();
  aos_writers("hours",
              po::value<float>(&benchmark_params.hours_write_perid)
                  ->default_value(benchmark_params.hours_write_perid),
              "write interval in hours");
  aos_writers("ids",
              po::value<size_t>(&benchmark_params.id_count)
                  ->default_value(benchmark_params.id_count),
              "total id count");
  aos_writers("wthreads",
              po::value<size_t>(&benchmark_params.total_threads_count)
                  ->default_value(benchmark_params.total_threads_count),
              "write threads count");
  aos_writers("freq", po::value<size_t>(&benchmark_params.freq_per_second)
                          ->default_value(benchmark_params.freq_per_second));
  desc.add(writers);

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

  if (vm.count("memory-only")) {
    std::cout << "memory-only" << std::endl;
    strategy = STRATEGY::MEMORY;
    memory_only = true;
  }

  if (vm.count("enable-readers")) {
    std::cout << "enable-readers" << std::endl;
    readers_enable = true;
  }

  if (vm.count("use-shard")) {
    std::cout << "use shard" << std::endl;
    use_shard = true;
  }

  if (vm.count("dont-repack")) {
    std::cout << "dont repack" << std::endl;
    dont_repack = true;
  }

  if (vm.count("readonly")) {
    std::cout << "Readonly mode." << std::endl;
    readonly = true;
  }

  if (vm.count("writeonly")) {
    write_only = true;
    std::cout << "Write only mode." << std::endl;
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

void show_info(IEngine *storage) {
  const auto OUT_SEP = ' ';
  clock_t t0 = clock();
  long long w0 = append_count.load();
  long long r0 = reads_count.load();
  while (true) {
    dariadb::utils::sleep_mls(100);

    long long w1 = append_count.load();
    long long r1 = reads_count.load();

    clock_t t1 = clock();
    auto step_time = double(double(t1 - t0) / (double)CLOCKS_PER_SEC);

    auto writes_per_sec = (w1 - w0) / step_time;
    summary_info->write_speed_metrics.push_back(writes_per_sec);
    auto reads_per_sec = (r1 - r0) / step_time;
    auto queue_sizes = storage->description();

    std::stringstream time_ss;
    time_ss << timeutil::to_string(write_time);

    std::stringstream stor_ss;
    stor_ss << "(";
    if (!memory_only) {
      stor_ss << "p:" << queue_sizes.pages_count << " w:" << queue_sizes.wal_count << " ";
    }

    stor_ss << "T:" << queue_sizes.active_works;

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
    persent_ss << (int64_t(100) * append_count) / benchmark_params.all_writes() << '%';

    std::stringstream drop_ss;
    if (!memory_only) {
      drop_ss << "[a:" << queue_sizes.dropper.wal << "]";
    }

    std::stringstream ss;

    ss // << "\r"
        << " T: " << std::setw(20) << std::setfill(OUT_SEP) << time_ss.str()
        << " store:" << stor_ss.str() << drop_ss.str() << " rd: " << reads_count
        << " s:" << read_speed_ss.str() << " wr: " << append_count
        << " s:" << write_speed_ss.str() << persent_ss.str();
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

void show_drop_info(IEngine *storage) {
  while (true) {
    dariadb::utils::sleep_mls(100);

    auto queue_sizes = storage->description();

    dariadb::logger_info(" storage: (p:", queue_sizes.pages_count,
                         " w:", queue_sizes.wal_count, " T:", queue_sizes.active_works,
                         ")", "[w:", queue_sizes.dropper.wal, "]");

    if (stop_info) {
      std::cout.flush();
      break;
    }
  }
  std::cout << "\n";
}

void reader(IMeasStorage *ms, IdSet all_id_set, Time from, Time to) {
  std::random_device r;
  std::default_random_engine e1(r());
  std::uniform_int_distribution<dariadb::Id> uniform_dist(from, to);

  while (true) {
    dariadb::utils::sleep_mls(10);
    BenchCallback clbk;
    clbk.count = 0;
    auto f = from;
    auto t = write_time;

    auto qi = dariadb::QueryInterval(
        dariadb::IdArray(all_id_set.begin(), all_id_set.end()), 0, f, t);
    ms->foreach (qi, &clbk);
    clbk.wait();
    reads_count += clbk.count;
    if (stop_readers) {
      break;
    }
  }
}

void rw_benchmark(IEngine *raw_ptr, Time start_time, IdSet &all_id_set) {

  std::thread info_thread(show_info, raw_ptr);

  std::vector<std::thread> writers(benchmark_params.total_threads_count);
  std::vector<std::thread> readers(benchmark_params.total_readers_count);

  size_t pos = 0;

  for (size_t i = 1; i < benchmark_params.total_threads_count + 1; i++) {
    auto id_from = dariadb_bench::get_id_from(benchmark_params, pos);
    auto id_to = dariadb_bench::get_id_to(benchmark_params, pos);
    for (size_t j = id_from; j < id_to; j++) {
      all_id_set.insert(j);
    }
    if (!readonly) {
      std::thread t{dariadb_bench::thread_writer_rnd_stor,
                    benchmark_params,
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
    for (size_t i = 1; i < benchmark_params.total_readers_count + 1; i++) {
      std::thread t{reader, raw_ptr, all_id_set, start_time, timeutil::current_time()};
      readers[pos++] = std::move(t);
    }
  }

  if (!readonly) {
    pos = 0;
    for (size_t i = 1; i < benchmark_params.total_threads_count + 1; i++) {
      std::thread t = std::move(writers[pos++]);
      t.join();
    }
  }

  if (readers_enable) {
    pos = 0;
    stop_readers = true;
    for (size_t i = 1; i < benchmark_params.total_readers_count + 1; i++) {
      std::thread t = std::move(readers[pos++]);
      t.join();
    }
  }

  stop_info = true;
  info_thread.join();
}

void read_all_bench(IEngine *ms, Time start_time, Time max_time, IdSet &all_id_set) {

  if (readonly) {
    start_time = Time(0);
  }
  BenchCallback clbk;

  QueryInterval qi{IdArray(all_id_set.begin(), all_id_set.end()), 0, start_time,
                   max_time};

  std::cout << "==> foreach all..." << std::endl;

  dariadb::utils::ElapsedTime et;

  ms->foreach (qi, &clbk);
  clbk.wait();

  auto elapsed = et.elapsed();
  std::cout << "readed: " << clbk.count << std::endl;
  std::cout << "time: " << elapsed << std::endl;
  summary_info->foreach_read_all_time = elapsed;

  if (readall_enabled) {
    std::cout << "==> read all..." << std::endl;

    dariadb::utils::ElapsedTime et;

    auto readed = ms->readInterval(qi);

    elapsed = et.elapsed();
    std::cout << "readed: " << readed.size() << std::endl;
    std::cout << "time: " << elapsed << std::endl;
    summary_info->read_all_time = elapsed;
    std::map<Id, MeasesList> _dict;
    for (auto &v : readed) {
      _dict[v.id].push_back(v);
    }

    if (readed.size() != benchmark_params.all_writes()) {
      std::cout << "expected: " << benchmark_params.all_writes() << " get:" << clbk.count
                << std::endl;
      std::cout << " all_writes: " << benchmark_params.all_writes();
      for (auto &kv : _dict) {
        std::cout << " " << kv.first << " -> " << kv.second.size() << std::endl;
      }
      throw MAKE_EXCEPTION("(clbk->count!=(iteration_count*total_threads_count))");
    }
  }
}

void check_engine_state(dariadb::storage::Settings_ptr settings, IEngine *raw_ptr) {
  std::cout << "==> Check storage state(" << strategy << ")... " << std::flush;
  if (use_shard) {
    std::cout << "OK" << std::endl;
    return;
  }
  auto files = raw_ptr->description();
  if (memory_only) {
    using dariadb::utils::fs::path_exists;
    if ((files.pages_count != files.wal_count && files.wal_count != size_t(0)) ||
        path_exists(storage_path)) {
      THROW_EXCEPTION("MEMONLY error: (p:", files.pages_count, " a:", files.wal_count,
                      " T:", files.active_works, ") exists: ", path_exists(storage_path));
    }
    return;
  }
  switch (strategy) {
  case dariadb::STRATEGY::WAL:
    if (files.pages_count != 0) {
      THROW_EXCEPTION("WAL error: (p:", files.pages_count, " a:", files.wal_count,
                      " T:", files.active_works, ")");
    }
    break;
  case dariadb::STRATEGY::COMPRESSED:
    if (files.wal_count >= 1 && files.pages_count == 0) {
      THROW_EXCEPTION("COMPRESSED error: (p:", files.pages_count, " a:", files.wal_count,
                      " T:", files.active_works, ")");
    }
    break;
  case dariadb::STRATEGY::MEMORY:
    if (files.wal_count != 0 && files.pages_count == 0) {
      THROW_EXCEPTION("MEMORY error: (p:", files.pages_count, " a:", files.wal_count,
                      " T:", files.active_works, ")");
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
  std::cout << "Writers count:" << benchmark_params.total_threads_count << std::endl;

  parse_cmdline(argc, argv);

  if (readers_enable) {
    std::cout << "Readers enable. count: " << benchmark_params.total_readers_count
              << std::endl;
  }
  summary_info = std::make_unique<dariadb_bench::BenchmarkSummaryInfo>(strategy);

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

    dariadb::storage::Settings_ptr settings = nullptr;
    if (!memory_only) {
      settings = dariadb::storage::Settings::create(storage_path);
      settings->strategy.setValue(strategy);
      /* settings->chunk_size.setValue(3072);
       settings->wal_file_size.setValue((1024 * 1024) * 64 / sizeof(dariadb::Meas));
       settings->wal_cache_size.setValue(4096 / sizeof(dariadb::Meas) * 30);
       settings->max_chunks_per_page.setValue(5 * 1024);
       settings->threads_in_common.setValue(5);*/
      settings->save();
    } else {
      settings = dariadb::storage::Settings::create();
    }

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
    IEngine_Ptr engine_ptr = nullptr;
    IEngine *raw_ptr = nullptr;
    if (use_shard) {
      auto s1_path = utils::fs::append_path(storage_path, "sh1");
      auto s2_path = utils::fs::append_path(storage_path, "sh2");
      {
        auto settings = dariadb::storage::Settings::create(s1_path);
        settings->strategy.setValue(strategy);
        settings->save();
      }

      {
        auto settings = dariadb::storage::Settings::create(s2_path);
        settings->strategy.setValue(strategy);
        settings->save();
      }
      engine_ptr = ShardEngine::create(storage_path);
      auto se = (ShardEngine *)engine_ptr.get();
      se->shardAdd({s1_path, "shard1", {Id(0), Id(1), Id(2), Id(3)}});
      se->shardAdd({s2_path, "shard2", IdSet()});
    } else {
      engine_ptr = IEngine_Ptr{new Engine(settings)};
    }
    raw_ptr = engine_ptr.get();

    if (is_exists) {
      raw_ptr->fsck();
    }

    dariadb::IdSet all_id_set;
    append_count = 0;
    stop_info = false;
    dariadb::utils::ElapsedTime wr_et;

    start_time = dariadb::timeutil::current_time() -
                 (boost::posix_time::seconds(1).total_milliseconds() * 60 * 60 *
                  benchmark_params.hours_write_perid);
    rw_benchmark(raw_ptr, start_time, all_id_set);

    auto writers_elapsed = wr_et.elapsed();
    stop_readers = true;

    std::cout << "total id:" << all_id_set.size() << std::endl;

    std::cout << "write time: " << writers_elapsed << std::endl;
    std::cout << "total speed: " << append_count / writers_elapsed << "/s" << std::endl;
    if (write_only) {
      return 0;
    }
    if (strategy != STRATEGY::MEMORY && strategy != STRATEGY::CACHE) {
      std::cout << "==> full flush..." << std::endl;
      stop_info = false;
      std::thread flush_info_thread(show_drop_info, raw_ptr);

      dariadb::utils::ElapsedTime et;
      raw_ptr->flush();

      { raw_ptr->wait_all_asyncs(); }
      auto elapsed = et.elapsed();
      stop_info = true;
      flush_info_thread.join();
      std::cout << "flush time: " << elapsed << std::endl;
    }

    check_engine_state(settings, raw_ptr);

    if (!readonly && !dont_repack && !memory_only) {
      if (strategy != dariadb::STRATEGY::MEMORY && strategy != STRATEGY::CACHE) {
        size_t ccount = size_t(raw_ptr->description().wal_count);
        std::cout << "==> drop part wals to " << ccount << "..." << std::endl;
        stop_info = false;
        std::thread flush_info_thread(show_drop_info, raw_ptr);

        dariadb::utils::ElapsedTime et;
        raw_ptr->compress_all();
        auto elapsed = et.elapsed();
        stop_info = true;
        flush_info_thread.join();
        std::cout << "drop time: " << elapsed << std::endl;
      }
      {
        auto pages_before = raw_ptr->description().pages_count;
        if (pages_before != 0) {
          std::cout << "==> pages before repack " << pages_before << "..." << std::endl;

          for (auto id : all_id_set) {
            dariadb::utils::ElapsedTime et;
            raw_ptr->repack(id);
            auto elapsed = et.elapsed();
            summary_info->repack_metrics.push_back(elapsed);
          }
          auto pages_after = raw_ptr->description().pages_count;

          std::cout << "==> pages after repack " << pages_after << "..." << std::endl;

          summary_info->page_repacked = pages_before - pages_after - 1;
          if (!use_shard) {
            if (strategy != STRATEGY::MEMORY && strategy != STRATEGY::CACHE &&
                pages_before <= pages_after) {
              THROW_EXCEPTION("pages_before <= pages_after");
            }
          }
        }
      }
    }

    auto queue_sizes = raw_ptr->description();
    std::cout << "\r"
              << " storage: (p:" << queue_sizes.pages_count
              << " a:" << queue_sizes.wal_count << ")" << std::endl;

    std::cout << "Active threads: "
              << utils::async::ThreadManager::instance()->active_works() << std::endl;

    summary_info->writed = append_count;
    dariadb_bench::readBenchmark(summary_info.get(), all_id_set, raw_ptr,
                                 read_benchmark_runs);

    auto max_time = raw_ptr->maxTime();
    std::cout << "==> interval end time: " << timeutil::to_string(max_time) << std::endl;

    read_all_bench(raw_ptr, start_time, max_time, all_id_set);

    if (!dont_repack && !memory_only) { // compaction
      std::cout << "compaction..." << std::endl;
      auto halfTime = (max_time - start_time) / 2;
      std::cout << "compaction period " << dariadb::timeutil::to_string(halfTime)
                << std::endl;
      for (auto id : all_id_set) {
        auto compaction_logic =
            std::make_unique<CompactionBenchmark>(id, halfTime, start_time, max_time);

        dariadb::utils::ElapsedTime et;
        raw_ptr->compact(compaction_logic.get());
        auto elapsed = et.elapsed();
        summary_info->compaction_metrics.push_back(elapsed);
      }
      BenchCallback clbk;
      QueryInterval qi{IdArray(all_id_set.begin(), all_id_set.end()), 0, start_time,
                       max_time};

      std::cout << "==> foreach all..." << std::endl;

      raw_ptr->foreach (qi, &clbk);
      clbk.wait();

      std::cout << "==> values after compaction: " << clbk.count << std::endl;
      if ((strategy != dariadb::STRATEGY::MEMORY &&
           strategy != dariadb::STRATEGY::CACHE) &&
          clbk.count == summary_info->writed) {
        throw std::logic_error("clbk.count == summary_info->writed");
      }
    }
    std::cout << "writed: " << append_count << std::endl;
    std::cout << "stoping storage...\n";
    engine_ptr = nullptr;
    settings = nullptr;
    auto blog = dynamic_cast<dariadb_bench::BenchmarkLogger *>(log_ptr.get());
    if (blog->_calls.load() == 0) {
      throw std::logic_error("log_ptr->_calls.load()==0");
    }

    std::cout << std::endl;
    benchmark_params.print();
    std::cout << "===" << std::endl;
    summary_info->print();
  }

  if (!(dont_clean || readonly) && (utils::fs::path_exists(storage_path))) {
    std::cout << "cleaning...\n";
    utils::fs::rm(storage_path);
  }
}
