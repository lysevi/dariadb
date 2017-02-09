#pragma once
#include <algorithm>
#include <atomic>
#include <iostream>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/strategy.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <random>
#include <tuple>

#include <boost/date_time/posix_time/posix_time.hpp>

namespace dariadb_bench {
struct BenchmarkSummaryInfo {
  size_t writed;
  double write_speed;
  double read_interval_speed;
  double read_timepoint_speed;
  double read_all_time;
  double join_all_time;
  size_t join_table_size;
  double foreach_read_all_time;
  double page_repack_time;
  size_t page_repacked;
  double stat_time;
  dariadb::STRATEGY strategy;

  BenchmarkSummaryInfo(dariadb::STRATEGY _strategy) {
    strategy = _strategy;
    writed = size_t(0);
    page_repacked = size_t(0);
    join_table_size = size_t(0);
    write_speed = read_interval_speed = read_timepoint_speed = read_all_time =
        stat_time = join_all_time = page_repack_time =
            foreach_read_all_time = 0.0;
  }

  void print() {
    std::cout << "benhcmark summary (" << strategy << ")" << std::endl;
    std::cout << "writed: " << writed << std::endl;
    std::cout << "write speed(average): " << write_speed << " per/sec"
              << std::endl;
    std::cout << "page repack: " << page_repack_time << " secs."
              << std::endl;
    std::cout << "page repacked: " << page_repacked << std::endl;
    std::cout << "read interval: " << read_interval_speed << " per/sec"
              << std::endl;
    std::cout << "stat: " << stat_time << " sec" << std::endl;
    std::cout << "read timepoint: " << read_timepoint_speed << " per/sec"
              << std::endl;
    std::cout << "read all: " << read_all_time << " secs." << std::endl;
    std::cout << "foreach all: " << foreach_read_all_time << " secs."
              << std::endl;
    std::cout << "join all: " << join_all_time << " secs." << std::endl;
    std::cout << "join size: " << join_table_size << std::endl;
  }
};

const size_t total_threads_count = 2;
const size_t hours_write_perid = 48;
const size_t writes_per_second = 2;
const size_t write_per_id_count =
    writes_per_second * 60 * 60 * hours_write_perid;
const size_t total_readers_count = 1;
const size_t id_count = 100;
const size_t id_per_thread = id_count / total_threads_count;
const uint64_t all_writes =
    total_threads_count * write_per_id_count * id_per_thread;

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

dariadb::Id get_id_from(dariadb::Id id) {
  return (id + 1) * id_per_thread - id_per_thread;
}

dariadb::Id get_id_to(dariadb::Id id) { return (id + 1) * id_per_thread; }

void thread_writer_rnd_stor(dariadb::Id id, std::atomic_llong *append_count,
                            dariadb::IMeasWriter *ms, dariadb::Time start_time,
                            dariadb::Time *write_time_time) {
  try {
    auto step = (boost::posix_time::seconds(1).total_milliseconds() /
                 writes_per_second);
    dariadb::Meas m;
    m.time = start_time;
    auto id_from = get_id_from(id);
    auto id_to = get_id_to(id);
    dariadb::logger("*** thread #", id, " id:[", id_from, " - ", id_to, "]");
    dariadb::IdSet ids;
    for (size_t i = 0; i < write_per_id_count; ++i) {
      m.flag = dariadb::Flag(id);
      m.time += step;
      *write_time_time = m.time;
      m.value = dariadb::Value(i);
      for (size_t j = id_from;
           j < id_to && i < dariadb_bench::write_per_id_count; j++) {
        m.id = j;
        ids.insert(m.id);
        if (ms->append(m).writed != 1) {
          std::cout << ">>> thread_writer_rnd_stor #" << id
                    << " can`t write new value. aborting." << std::endl;
          return;
        }
        (*append_count)++;
      }
    }

    std::stringstream ss;
    for (auto id : ids) {
      ss << " " << id;
    }
    dariadb::logger("*** thread #", id, " ids: ", ss.str());
  } catch (...) {
    std::cerr << "thread id=#" << id << " catch error!!!!" << std::endl;
  }
}

void readBenchmark(BenchmarkSummaryInfo *summary_info,
                   const dariadb::IdSet &all_id_set,
                   dariadb::IMeasStorage *stor, size_t reads_count,
                   bool quiet = false, bool check_is_end = true) {
  std::cout << "==> init random ids...." << std::endl;
  dariadb::IdArray random_ids{all_id_set.begin(), all_id_set.end()};
  std::random_shuffle(random_ids.begin(), random_ids.end());
  dariadb::IdArray current_ids{1};

  using Id2Times = std::tuple<dariadb::Id, dariadb::Time, dariadb::Time>;
  std::vector<Id2Times> interval_queries{reads_count};
  size_t cur_id = 0;

  std::cout << "==> init random intervals...." << std::endl;
  {
    auto start = clock();

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

    auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC) / reads_count;
	
    if (!quiet) {
      std::cout << "time: " << elapsed << std::endl;
    }
  }

  std::cout << "==> stat...." << std::endl;
  {//stat
	  auto start = clock();

	  for (size_t i = 0; i < reads_count; i++) {
		  Id2Times curval = interval_queries[i];
		  stor->stat(std::get<0>(curval), std::get<1>(curval), std::get<2>(curval));
	  }

	  auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC) / reads_count;
	  summary_info->stat_time = elapsed;
	  if (!quiet) {
		  std::cout << "time: " << elapsed << std::endl;
	  }
  }
  std::random_device r;
  std::default_random_engine e1(r());

  {
    if (!quiet) {
      std::cout << "==> time point reads..." << std::endl;
    }

    auto start = clock();

    for (size_t i = 0; i < reads_count; i++) {
      Id2Times curval = interval_queries[i];
      std::uniform_int_distribution<dariadb::Time> uniform_dist(
          std::get<1>(curval), std::get<2>(curval));
      auto time_point = uniform_dist(e1);
      current_ids[0] = std::get<0>(curval);
      cur_id = (cur_id + 1) % random_ids.size();

      dariadb::QueryTimePoint qp{current_ids, 0, time_point};
      stor->readTimePoint(qp);
    }
    auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC) / reads_count;
    summary_info->read_timepoint_speed = elapsed;
    if (!quiet) {
      std::cout << "time: " << elapsed << std::endl;
    }
  }
  std::list<std::tuple<dariadb::Time, dariadb::Time>> interval_list;
  {
    if (!quiet) {
      std::cout << "==> intervals foreach..." << std::endl;
    }

    auto start = clock();
    cur_id = 0;

    size_t total_count = 0;
    for (size_t i = 0; i < reads_count; i++) {
      std::shared_ptr<BenchCallback> clbk{new BenchCallback};

      Id2Times curval = interval_queries[i];
      std::uniform_int_distribution<dariadb::Time> uniform_dist(
          std::get<1>(curval), std::get<2>(curval));
      auto time_point1 = uniform_dist(e1);
      auto time_point2 = uniform_dist(e1);
      auto f = std::min(time_point1, time_point2);
      auto t = std::max(time_point1, time_point2);
      interval_list.push_back(std::tie(f, t));

      current_ids[0] = std::get<0>(curval);
      cur_id = (cur_id + 1) % random_ids.size();
      auto qi = dariadb::QueryInterval(current_ids, 0, f, t);
      stor->foreach (qi, clbk.get());
      if (check_is_end) {
        clbk->wait();
      }
      total_count += clbk->count;
    }
    auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC) / reads_count;
    summary_info->read_interval_speed = elapsed;
    if (!quiet) {
      std::cout << "time: " << elapsed
                << " average count: " << total_count / reads_count << std::endl;
    }
  }

  {
    if (!quiet) {
      std::cout << "==> intervals reads..." << std::endl;
    }

    auto start = clock();
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
    auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC) / reads_count;
    if (!quiet) {
      std::cout << "time: " << elapsed
                << " average count: " << count / reads_count << std::endl;
    }
  }
}
}
