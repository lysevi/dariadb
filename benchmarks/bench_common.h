#pragma once
#include <dariadb.h>
#include <algorithm>
#include <atomic>
#include <random>
#include <tuple>

namespace dariadb_bench {

const size_t total_threads_count = 5;
const size_t iteration_count = 3000000;
const size_t total_readers_count = 1;
const size_t id_per_thread = 1;

class BenchCallback : public dariadb::storage::ReaderClb {
public:
  BenchCallback() { count = 0; }
  void call(const dariadb::Meas &) { count++; }
  size_t count;
};

dariadb::Id get_id_from(dariadb::Id id) {
  return (id + 1) * id_per_thread - id_per_thread;
}

dariadb::Id get_id_to(dariadb::Id id) {
  return (id + 1) * id_per_thread;
}
void thread_writer_rnd_stor(dariadb::Id id, dariadb::Time sleep_time,
                            std::atomic_long *append_count,
                            dariadb::storage::MeasWriter *ms) {
  try {
    auto m = dariadb::Meas::empty();
    m.time = dariadb::timeutil::current_time();
    auto id_from = get_id_from(id);
    auto id_to = get_id_to(id);
	size_t i = 0;
    while(i<iteration_count){
      m.flag = dariadb::Flag(id);
      m.src = dariadb::Flag(id);
      m.time += sleep_time;
      m.value = dariadb::Value(i);
      for (size_t j = id_from; j < id_to && i < dariadb_bench::iteration_count; j++, i++) {
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

void readBenchark(const dariadb::IdSet &all_id_set, dariadb::storage::MeasStorage *stor,
                  size_t reads_count, dariadb::Time from, dariadb::Time to,
                  bool quiet = false) {
  std::cout << "init random ids...." << std::endl;
  dariadb::IdArray random_ids{all_id_set.begin(), all_id_set.end()};
  std::random_shuffle(random_ids.begin(), random_ids.end());
  dariadb::IdArray current_ids{1};
  
  using Id2Times = std::tuple<dariadb::Id, dariadb::Time, dariadb::Time>;
  std::vector<Id2Times> interval_queries{ reads_count };
  size_t cur_id = 0;

  std::cout << "init random intervals...." << std::endl;
  {
    auto start = clock();

    for (size_t i = 0; i < reads_count; i++) {
      auto id = random_ids[cur_id];
      cur_id = (cur_id + 1) % random_ids.size();
      dariadb::Time minT, maxT;
      if (!stor->minMaxTime(id, &minT, &maxT)) {
        throw MAKE_EXCEPTION("id not found");
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
      std::cout << "time point reads..." << std::endl;
    }

    std::shared_ptr<BenchCallback> clbk{new BenchCallback};

    auto start = clock();

    for (size_t i = 0; i < reads_count; i++) {
      Id2Times curval = interval_queries[i];
      std::uniform_int_distribution<dariadb::Time> uniform_dist(std::get<1>(curval),
                                                                std::get<2>(curval));
      auto time_point = uniform_dist(e1);
	  current_ids[0] = std::get<0>(curval);
      cur_id = (cur_id + 1) % random_ids.size();

      dariadb::storage::QueryTimePoint qp{current_ids, 0, time_point};
      stor->readInTimePoint(qp)->readAll(clbk.get());
    }
    auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC) / reads_count;
    if (!quiet) {
      std::cout << "time: " << elapsed << std::endl;
    }
  }
  {
    if (!quiet) {
      std::cout << "intervals reads..." << std::endl;
    }

    std::shared_ptr<BenchCallback> clbk{new BenchCallback};

    auto start = clock();
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
      stor->readInterval(qi)->readAll(clbk.get());
    }
    auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC) / reads_count;
    if (!quiet) {
      std::cout << "time: " << elapsed << " average count: " << clbk->count / reads_count
                << std::endl;
    }
  }
}
}
