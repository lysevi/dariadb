#pragma once
#include <libdariadb/engines/strategy.h>
#include <libdariadb/interfaces/imeasstorage.h>

#include <atomic>
#include <list>
#include <tuple>
#include <vector>

namespace dariadb_bench {

struct BenchmarkSummaryInfo {
  struct SpeedMetric {
    double min;
    double max;
    double average;
    double median;
    double p90;
    double sigma;
    std::string name;
  };
  size_t writed;
  double read_all_time;
  double foreach_read_all_time;
  size_t page_repacked;
  dariadb::STRATEGY strategy;

  std::list<double> write_speed_metrics;
  std::list<double> read_timepoint_speed_metrics;
  std::list<double> read_interval_speed_metrics;
  std::list<double> full_read_timeseries_metrics;
  std::list<double> copy_to_metrics;
  std::list<double> stat_metrics;
  std::list<double> compaction_metrics;
  std::list<double> repack_metrics;

  BenchmarkSummaryInfo(dariadb::STRATEGY _strategy) {
    strategy = _strategy;
    writed = size_t(0);
    page_repacked = size_t(0);
    read_all_time = foreach_read_all_time = 0.0;
  }

  void print();
};

struct BenchmarkParams {
  size_t total_threads_count = 2;
  float hours_write_perid = 48;
  size_t freq_per_second = 2;
  size_t total_readers_count = 1;
  size_t id_count = 100;

  size_t id_per_thread() const { return id_count / total_threads_count; }

  size_t write_per_id_count() const {
    return (size_t)(freq_per_second * 60 * 60 * hours_write_perid);
  }

  uint64_t all_writes() const {
    return total_threads_count * write_per_id_count() * id_per_thread();
  }

  void print();
};

dariadb::Id get_id_from(BenchmarkParams params, dariadb::Id id);
dariadb::Id get_id_to(BenchmarkParams params, dariadb::Id id);

class BenchmarkLogger : public dariadb::utils::ILogger {
public:
  std::atomic<uint64_t> _calls;
  void message(dariadb::utils::LOG_MESSAGE_KIND kind, const std::string &msg);
};

void thread_writer_rnd_stor(BenchmarkParams params, dariadb::Id id,
                            std::atomic_llong *append_count, dariadb::IMeasWriter *ms,
                            dariadb::Time start_time, dariadb::Time *write_time_time);

void readBenchmark(BenchmarkSummaryInfo *summary_info, const dariadb::IdSet &all_id_set,
                   dariadb::IMeasStorage *stor, size_t reads_count);
} // namespace dariadb_bench
