#pragma once
#include <libdariadb/engines/strategy.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/statistic/functions.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <libdariadb/utils/utils.h>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <algorithm>
#include <atomic>
#include <iomanip>
#include <iostream>
#include <list>
#include <random>
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
  double page_repack_time;
  size_t page_repacked;
  dariadb::STRATEGY strategy;

  std::list<double> write_speed_metrics;
  std::list<double> read_timepoint_speed_metrics;
  std::list<double> read_interval_speed_metrics;
  std::list<double> full_read_timeseries_metrics;
  std::list<double> copy_to_metrics;
  std::list<double> stat_metrics;
  std::list<double> compaction_metrics;
  BenchmarkSummaryInfo(dariadb::STRATEGY _strategy) {
    strategy = _strategy;
    writed = size_t(0);
    page_repacked = size_t(0);
    read_all_time = page_repack_time = foreach_read_all_time = 0.0;
  }
  // TODO move to cpp file
  void print(const std::vector<SpeedMetric> &metrics) {
    using namespace std;
    const char separator = ' ';
    const std::string nameName = "name";
    const std::string nameMin = "min";
    const std::string nameMax = "max";
    const std::string nameAv = "average";
    const std::string nameMedian = "median";
    const std::string nameP90 = "percentile";
    const std::string nameSigma = "sigma";
    size_t nameWidth = nameName.size();
    size_t minWidth = nameMin.size();
    size_t maxWidth = nameMax.size();
    size_t avWidth = nameAv.size();
    size_t medWidth = nameMedian.size();
    size_t p90Width = nameP90.size();
    size_t sigmaWidth = nameSigma.size();

    auto align_base = right;
    for (auto sm : metrics) {
      nameWidth = std::max(nameWidth, sm.name.size());
      minWidth = std::max(minWidth, std::to_string(sm.min).size());
      maxWidth = std::max(maxWidth, std::to_string(sm.max).size());
      avWidth = std::max(avWidth, std::to_string(sm.average).size());
      medWidth = std::max(medWidth, std::to_string(sm.median).size());
      p90Width = std::max(p90Width, std::to_string(sm.p90).size());
      sigmaWidth = std::max(sigmaWidth, std::to_string(sm.sigma).size());
    }
    nameWidth += 1;
    minWidth += 1;
    maxWidth += 1;
    avWidth += 1;
    medWidth += 1;
    sigmaWidth += 1;

    cout << left << setw(nameWidth) << setfill(separator) << nameName;
    cout << align_base << setw(minWidth) << setfill(separator) << nameMin;
    cout << align_base << setw(maxWidth) << setfill(separator) << nameMax;
    cout << align_base << setw(avWidth) << setfill(separator) << nameAv;
    cout << align_base << setw(medWidth) << setfill(separator) << nameMedian;
    cout << align_base << setw(p90Width) << setfill(separator) << nameP90;
    cout << align_base << setw(sigmaWidth) << setfill(separator) << nameSigma;
    cout << std::endl;
    for (auto sm : metrics) {
      cout << left << setw(nameWidth) << setfill(separator) << sm.name;
      cout << align_base << setw(minWidth) << setfill(separator) << sm.min;
      cout << align_base << setw(maxWidth) << setfill(separator) << sm.max;
      cout << align_base << setw(avWidth) << setfill(separator) << sm.average;
      cout << align_base << setw(medWidth) << setfill(separator) << sm.median;
      cout << align_base << setw(p90Width) << setfill(separator) << sm.p90;
      cout << align_base << setw(sigmaWidth) << setfill(separator) << sm.sigma;
      cout << std::endl;
    }
  }

  void print() {

    std::cout << "benhcmark summary (" << strategy << ")" << std::endl;
    std::cout << "writed: " << writed << std::endl;
    {
      SpeedMetric write_sm = metrics("write speed", write_speed_metrics);
      SpeedMetric read_tp_sm = metrics("read timepoint", read_timepoint_speed_metrics);
      SpeedMetric read_interval_sm =
          metrics("read interval", read_interval_speed_metrics);
      SpeedMetric full_read_interval_sm =
          metrics("read timeseries", full_read_timeseries_metrics);
      SpeedMetric copy_sm = metrics("copy timeseries", copy_to_metrics);
      SpeedMetric stat_sm = metrics("stat", stat_metrics);
      SpeedMetric compact_sm = metrics("compaction", compaction_metrics);
      std::vector<SpeedMetric> all_metrics{
          write_sm, read_tp_sm, read_interval_sm, full_read_interval_sm,
          copy_sm,  stat_sm,    compact_sm};
      print(all_metrics);
    }

    std::cout << "page repack: " << page_repack_time << " secs." << std::endl;
    std::cout << "page repacked: " << page_repacked << std::endl;

    std::cout << "read all: " << read_all_time << " secs." << std::endl;
    std::cout << "foreach all: " << foreach_read_all_time << " secs." << std::endl;
  }

  SpeedMetric metrics(const std::string &name, const std::list<double> &values_list) {
    std::vector<double> values(values_list.begin(), values_list.end());
    std::vector<dariadb::Meas> meases;
    meases.reserve(values.size());

    dariadb::statistic::Average av("average");
    dariadb::statistic::Median md("median");
    dariadb::statistic::Percentile90 p90("p90");
    dariadb::statistic::StandartDeviation sigma("sigma");

    SpeedMetric result;
    result.name = name;
    result.min = dariadb::MAX_VALUE;
    result.max = dariadb::MIN_VALUE;

    for (auto v : values) {
      dariadb::Meas m;
      m.value = v;
      meases.push_back(m);
      result.min = std::min(result.min, v);
      result.max = std::max(result.max, v);
    }

    result.average = av.apply(meases).value;
    result.median = md.apply(meases).value;
    result.p90 = p90.apply(meases).value;
    result.sigma = sigma.apply(meases).value;
    return result;
  }
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

  void print() {
    std::cout << "Benchmark params:" << std::endl;
    std::cout << "writers: " << total_threads_count << std::endl;
    std::cout << "hours: " << hours_write_perid << std::endl;
    std::cout << "write frequency: " << freq_per_second << std::endl;
    std::cout << "id count: " << id_count << std::endl;
    std::cout << "readers: " << total_readers_count << std::endl;
    std::cout << "id per thread: " << id_per_thread() << std::endl;
    std::cout << "values per time series: " << write_per_id_count() << std::endl;
    std::cout << "total writes: " << all_writes() << std::endl;
  }
};

class BenchmarkLogger : public dariadb::utils::ILogger {
public:
  std::atomic<uint64_t> _calls;
  void message(dariadb::utils::LOG_MESSAGE_KIND kind, const std::string &msg) {
    _calls += 1;
    auto ct = dariadb::timeutil::current_time();
    auto ct_str = dariadb::timeutil::to_string(ct);
    std::stringstream ss;
    ss << ct_str << " ";
    switch (kind) {
    case dariadb::utils::LOG_MESSAGE_KIND::FATAL:
      ss << "[err] " << msg << std::endl;
      break;
    case dariadb::utils::LOG_MESSAGE_KIND::INFO:
      ss << "[inf] " << msg << std::endl;
      break;
    case dariadb::utils::LOG_MESSAGE_KIND::MESSAGE:
      ss << "[dbg] " << msg << std::endl;
      break;
    }
    std::cout << ss.str();
  }
};

class BenchCallback : public dariadb::IReadCallback {
public:
  BenchCallback() {
    count = 0;
    is_end_called = false;
  }
  void apply(const dariadb::Meas &) override { count++; }
  void is_end() override {
    is_end_called = true;
    dariadb::IReadCallback::is_end();
  }
  std::mutex _locker;
  size_t count;
  bool is_end_called;
};

class BenchWriteCallback : public dariadb::IReadCallback {
public:
  BenchWriteCallback(dariadb::IMeasStorage *store) {
    count = 0;
    is_end_called = false;
    storage = store;
  }
  void apply(const dariadb::Meas &m) override {
    count++;
    dariadb::Meas nm = m;
    nm.id = dariadb::MAX_ID - m.id;

    ENSURE(storage != nullptr);

    storage->append(nm);
  }

  void is_end() override {
    is_end_called = true;
    IReadCallback::is_end();
  }
  std::atomic<size_t> count;
  bool is_end_called;
  dariadb::IMeasStorage *storage;
};

dariadb::Id get_id_from(BenchmarkParams params, dariadb::Id id) {
  return (id + 1) * params.id_per_thread() - params.id_per_thread();
}

dariadb::Id get_id_to(BenchmarkParams params, dariadb::Id id) {
  return (id + 1) * params.id_per_thread();
}

void thread_writer_rnd_stor(BenchmarkParams params, dariadb::Id id,
                            std::atomic_llong *append_count, dariadb::IMeasWriter *ms,
                            dariadb::Time start_time, dariadb::Time *write_time_time) {
  try {
    auto step =
        (boost::posix_time::seconds(1).total_milliseconds() / params.freq_per_second);
    dariadb::Meas m;
    m.time = start_time;
    auto id_from = get_id_from(params, id);
    auto id_to = get_id_to(params, id);
    dariadb::logger("*** thread #", id, " id:[", id_from, " - ", id_to, "]");
    for (size_t i = 0; i < params.write_per_id_count(); ++i) {
      m.flag = dariadb::Flag(id);
      m.time += step;
      *write_time_time = m.time;
      m.value = dariadb::Value(i);
      for (size_t j = id_from; j < id_to && i < params.write_per_id_count(); j++) {
        m.id = j;
        if (ms->append(m).writed != 1) {
          std::cout << ">>> thread_writer_rnd_stor #" << id
                    << " can`t write new value. aborting." << std::endl;
          return;
        }
        append_count->fetch_add(1);
      }
    }

    /* std::stringstream ss;
     for (auto id : ids) {
       ss << " " << id;
     }
     dariadb::logger("*** thread #", id, " ids: ", ss.str());*/
  } catch (...) {
    std::cerr << "thread id=#" << id << " catch error!!!!" << std::endl;
  }
}

void readBenchmark(BenchmarkSummaryInfo *summary_info, const dariadb::IdSet &all_id_set,
                   dariadb::IMeasStorage *stor, size_t reads_count) {
  std::cout << "==> init random ids...." << std::endl;
  dariadb::IdArray random_ids{all_id_set.begin(), all_id_set.end()};
  std::random_shuffle(random_ids.begin(), random_ids.end());
  dariadb::IdArray current_ids{1};

  using Id2Times = std::tuple<dariadb::Id, dariadb::Time, dariadb::Time>;
  std::vector<Id2Times> interval_queries{reads_count};
  size_t cur_id = 0;

  std::cout << "==> init random intervals...." << std::endl;
  {
    dariadb::utils::ElapsedTime et;

    for (size_t i = 0; i < reads_count; i++) {
      auto id = random_ids[cur_id];
      cur_id = (cur_id + 1) % random_ids.size();
      dariadb::Time minT, maxT;

      if (!stor->minMaxTime(id, &minT, &maxT)) {
        std::stringstream ss;
        ss << "id " << id << " not found";
        std::cout << ss.str() << std::endl;
        --i;
        continue;
        // throw MAKE_EXCEPTION(ss.str());
      }
      interval_queries[i] = std::tie(id, minT, maxT);
    }

    auto elapsed = et.elapsed();

    std::cout << "time: " << elapsed << std::endl;
  }

  std::cout << "==> stat...." << std::endl;
  { // stat

    for (size_t i = 0; i < reads_count; i++) {
      Id2Times curval = interval_queries[i];
      dariadb::utils::ElapsedTime et;
      stor->stat(std::get<0>(curval), std::get<1>(curval), std::get<2>(curval));
      auto elapsed = et.elapsed();
      summary_info->stat_metrics.push_back(elapsed);
    }
  }
  std::random_device r;
  std::default_random_engine e1(r());

  {

    std::cout << "==> time point reads..." << std::endl;

    for (size_t i = 0; i < reads_count; i++) {
      Id2Times curval = interval_queries[i];
      std::uniform_int_distribution<dariadb::Time> uniform_dist(std::get<1>(curval),
                                                                std::get<2>(curval));
      auto time_point = uniform_dist(e1);
      current_ids[0] = std::get<0>(curval);
      cur_id = (cur_id + 1) % random_ids.size();

      dariadb::QueryTimePoint qp{current_ids, 0, time_point};
      dariadb::utils::ElapsedTime et;
      stor->readTimePoint(qp);
      auto elapsed = et.elapsed();
      summary_info->read_timepoint_speed_metrics.push_back(elapsed);
    }
  }
  std::list<std::tuple<dariadb::Time, dariadb::Time>> interval_list;
  {

    std::cout << "==> intervals foreach..." << std::endl;

    cur_id = 0;

    size_t total_count = 0;
    for (size_t i = 0; i < reads_count; i++) {
      BenchCallback clbk;

      Id2Times curval = interval_queries[i];
      std::uniform_int_distribution<dariadb::Time> uniform_dist(std::get<1>(curval),
                                                                std::get<2>(curval));
      auto time_point1 = uniform_dist(e1);
      auto time_point2 = uniform_dist(e1);
      auto f = std::min(time_point1, time_point2);
      auto t = std::max(time_point1, time_point2);
      interval_list.push_back(std::tie(f, t));

      current_ids[0] = std::get<0>(curval);
      cur_id = (cur_id + 1) % random_ids.size();
      dariadb::utils::ElapsedTime et;
      auto qi = dariadb::QueryInterval(current_ids, 0, f, t);
      stor->foreach (qi, &clbk);

      clbk.wait();

      auto elapsed = et.elapsed();
      summary_info->read_interval_speed_metrics.push_back(elapsed);
      total_count += clbk.count;
    }
  }

  {

    std::cout << "==> intervals foreach(full)..." << std::endl;

    cur_id = 0;

    size_t total_count = 0;
    for (size_t i = 0; i < reads_count; i++) {
      BenchCallback clbk;

      Id2Times curval = interval_queries[i];
      auto time_point1 = std::get<1>(curval);
      auto time_point2 = std::get<2>(curval);
      auto f = std::min(time_point1, time_point2);
      auto t = std::max(time_point1, time_point2);
      interval_list.push_back(std::tie(f, t));

      current_ids[0] = std::get<0>(curval);
      cur_id = (cur_id + 1) % random_ids.size();
      auto qi = dariadb::QueryInterval(current_ids, 0, f, t);
      dariadb::utils::ElapsedTime et;
      stor->foreach (qi, &clbk);

      clbk.wait();
      auto elapsed = et.elapsed();
      summary_info->full_read_timeseries_metrics.push_back(elapsed);
      total_count += clbk.count;
    }
  }
  {

    std::cout << "==> intervals foreach(copy)..." << std::endl;

    cur_id = 0;

    size_t total_count = 0;
    for (size_t i = 0; i < reads_count; i++) {
      BenchWriteCallback clbk(stor);

      Id2Times curval = interval_queries[i];
      auto time_point1 = std::get<1>(curval);
      auto time_point2 = std::get<2>(curval);
      auto f = std::min(time_point1, time_point2);
      auto t = std::max(time_point1, time_point2);
      interval_list.push_back(std::tie(f, t));

      current_ids[0] = std::get<0>(curval);
      cur_id = (cur_id + 1) % random_ids.size();
      auto qi = dariadb::QueryInterval(current_ids, 0, f, t);
      dariadb::utils::ElapsedTime et;
      stor->foreach (qi, &clbk);

      clbk.wait();
      auto elapsed = et.elapsed();
      summary_info->copy_to_metrics.push_back(elapsed);
      total_count += clbk.count;
    }
  }
  {

    std::cout << "==> intervals reads..." << std::endl;

    dariadb::utils::ElapsedTime et;
    size_t count = 0;
    cur_id = 0;
    auto it = interval_list.begin();
    for (size_t i = 0; i < reads_count; i++) {
      Id2Times curval = interval_queries[i];
      auto f = std::get<0>(*it);
      auto t = std::get<1>(*it);
      ++it;

      current_ids[0] = std::get<0>(curval);
      cur_id = (cur_id + 1) % random_ids.size();
      auto qi = dariadb::QueryInterval(current_ids, 0, f, t);
      count += stor->readInterval(qi).size();
    }
    auto elapsed = et.elapsed();

    std::cout << "time: " << elapsed << " average count: " << count / reads_count
              << std::endl;
  }
}
} // namespace dariadb_bench
