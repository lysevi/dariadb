#pragma once
#include <dariadb.h>
#include <atomic>
#include <random>

namespace dariadb_bench {
// TODO use cmd line params
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
	return (id + 1)*id_per_thread - id_per_thread;
}

dariadb::Id get_id_to(dariadb::Id id) {
	return (id + 1)*id_per_thread;
}
void thread_writer_rnd_stor(dariadb::Id id, dariadb::Time sleep_time,
                            std::atomic_long *append_count,
                            dariadb::storage::MeasWriter *ms) {
  auto m = dariadb::Meas::empty();
  m.time = dariadb::timeutil::current_time();
  auto id_from = get_id_from(id);
  auto id_to = get_id_to(id);
  for (size_t i = 0; i < dariadb_bench::iteration_count; i++) {
    m.flag = dariadb::Flag(id);
    m.src = dariadb::Flag(id);
    m.time += sleep_time;
    m.value = dariadb::Value(i);
	for (size_t j = id_from; j < id_to; j++) {
		m.id = j;
		if (ms->append(m).writed != 1) {
			return;
		}
	}
    (*append_count)++;
  }
}

void readBenchark(const dariadb::IdSet &all_id_set,
                  dariadb::storage::MeasStorage_ptr stor, size_t reads_count,
                  dariadb::Time from, dariadb::Time to, bool quiet = false) {
  {
    if (!quiet) {
      std::cout << "time point reads..." << std::endl;
    }
    std::random_device r;
    std::default_random_engine e1(r());
    std::uniform_int_distribution<dariadb::Id> uniform_dist(from, to);

    std::shared_ptr<BenchCallback> clbk{new BenchCallback};

    auto start = clock();

    for (size_t i = 0; i < reads_count; i++) {
      auto time_point = uniform_dist(e1);
      dariadb::storage::QueryTimePoint qp{
          dariadb::IdArray(all_id_set.begin(), all_id_set.end()), 0, time_point};
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
    std::random_device r;
    std::default_random_engine e1(r());
    std::uniform_int_distribution<dariadb::Id> uniform_dist(stor->minTime(),
                                                            stor->maxTime());

    std::shared_ptr<BenchCallback> clbk{new BenchCallback};

    auto start = clock();

    for (size_t i = 0; i < reads_count; i++) {
      auto time_point1 = uniform_dist(e1);
      auto time_point2 = uniform_dist(e1);
      auto f = std::min(time_point1, time_point2);
      auto t = std::max(time_point1, time_point2);
      auto qi = dariadb::storage::QueryInterval(
          dariadb::IdArray(all_id_set.begin(), all_id_set.end()), 0, f, t);
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
