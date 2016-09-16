#pragma once
#include <algorithm>
#include <atomic>
#include <libdariadb/interfaces/imeasstorage.h>
#include <iostream>
#include <random>
#include <sstream>
#include <libdariadb/timeutil.h>
#include <tuple>
#include <libdariadb/utils/metrics.h>
#include <libdariadb/utils/thread_manager.h>

namespace dariadb_bench {

const size_t total_threads_count = 2;
const size_t hours_write_perid = 48;
const size_t writes_per_second = 2;
const size_t write_per_id_count = writes_per_second * 60 * 60 * hours_write_perid;
const size_t total_readers_count = 1;
const size_t id_per_thread = 100 / total_threads_count;
// const size_t total_threads_count = 5;
// const size_t hours_write_perid = 1;
// const size_t writes_per_second = 2;
// const size_t write_per_id_count = writes_per_second * 60 * 60 * hours_write_perid;
// const size_t total_readers_count = 1;
// const size_t id_per_thread = 200000;
const uint64_t all_writes = total_threads_count * write_per_id_count * id_per_thread;

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

class BenchCallback : public dariadb::storage::IReaderClb {
public:
  BenchCallback() {
    count = 0;
    is_end_called = false;
  }
  void call(const dariadb::Meas &) { count++; }
  void is_end() override { is_end_called = true; }
  size_t count;
  bool is_end_called;
};

dariadb::Id get_id_from(dariadb::Id id) {
  return (id + 1) * id_per_thread - id_per_thread;
}

dariadb::Id get_id_to(dariadb::Id id) {
  return (id + 1) * id_per_thread;
}
void thread_writer_rnd_stor(dariadb::Id id, std::atomic_llong *append_count,
                            dariadb::storage::IMeasWriter *ms, dariadb::Time start_time,
                            dariadb::Time *write_time_time) {
  try {
    auto m = dariadb::Meas::empty();
    m.time = start_time;
    auto id_from = get_id_from(id);
    auto id_to = get_id_to(id);

    for (size_t i = 0; i < write_per_id_count; ++i) {
      m.flag = dariadb::Flag(id);
      m.src = dariadb::Flag(id);
      m.time += 1000 / writes_per_second;
      *write_time_time = m.time;
      m.value = dariadb::Value(i);
      for (size_t j = id_from; j < id_to && i < dariadb_bench::write_per_id_count; j++) {
        m.id = j;
        if (ms->append(m).writed != 1) {
          return;
        }
        (*append_count)++;
      }
    }
  } catch (...) {
    std::cerr << "thread id=#" << id << " catch error!!!!" << std::endl;
  }
}

void readBenchark(const dariadb::IdSet &all_id_set, dariadb::storage::IMeasStorage *stor,
                  size_t reads_count, bool quiet = false) {
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
  std::random_device r;
  std::default_random_engine e1(r());

  {
    if (!quiet) {
      std::cout << "==> time point reads..." << std::endl;
    }

    auto start = clock();

    for (size_t i = 0; i < reads_count; i++) {
      Id2Times curval = interval_queries[i];
      std::uniform_int_distribution<dariadb::Time> uniform_dist(std::get<1>(curval),
                                                                std::get<2>(curval));
      auto time_point = uniform_dist(e1);
      current_ids[0] = std::get<0>(curval);
      cur_id = (cur_id + 1) % random_ids.size();

      dariadb::storage::QueryTimePoint qp{current_ids, 0, time_point};
      stor->readTimePoint(qp);
    }
    auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC) / reads_count;
    if (!quiet) {
      std::cout << "time: " << elapsed << std::endl;
    }
  }
  {
    if (!quiet) {
      std::cout << "==> intervals foreach..." << std::endl;
    }

    auto start = clock();
    cur_id = 0;
    std::shared_ptr<BenchCallback> clbk{new BenchCallback};
    for (size_t i = 0; i < reads_count; i++) {

      Id2Times curval = interval_queries[i];
      std::uniform_int_distribution<dariadb::Time> uniform_dist(std::get<1>(curval),
                                                                std::get<2>(curval));
      auto time_point1 = uniform_dist(e1);
      auto time_point2 = uniform_dist(e1);
      auto f = std::min(time_point1, time_point2);
      auto t = std::max(time_point1, time_point2);

      current_ids[0] = std::get<0>(curval);
      cur_id = (cur_id + 1) % random_ids.size();
      auto qi = dariadb::storage::QueryInterval(current_ids, 0, f, t);
      stor->foreach (qi, clbk.get());
    }
    auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC) / reads_count;
    if (!quiet) {
      std::cout << "time: " << elapsed << " average count: " << clbk->count / reads_count
                << std::endl;
    }
    if (!clbk->is_end_called) {
      THROW_EXCEPTION_SS("benchmark: logic error - !clbk->is_end_called");
    }
  }

  {
    if (!quiet) {
      std::cout << "==> intervals reads..." << std::endl;
    }

    auto start = clock();
    size_t count = 0;
    cur_id = 0;
    for (size_t i = 0; i < reads_count; i++) {
      Id2Times curval = interval_queries[i];
      std::uniform_int_distribution<dariadb::Time> uniform_dist(std::get<1>(curval),
                                                                std::get<2>(curval));
      auto time_point1 = uniform_dist(e1);
      auto time_point2 = uniform_dist(e1);
      auto f = std::min(time_point1, time_point2);
      auto t = std::max(time_point1, time_point2);

      current_ids[0] = std::get<0>(curval);
      cur_id = (cur_id + 1) % random_ids.size();
      auto qi = dariadb::storage::QueryInterval(current_ids, 0, f, t);
      count += stor->readInterval(qi).size();
    }
    auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC) / reads_count;
    if (!quiet) {
      std::cout << "time: " << elapsed << " average count: " << count / reads_count
                << std::endl;
    }
  }
}
}
