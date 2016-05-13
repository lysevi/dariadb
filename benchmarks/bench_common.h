#pragma once
#include <dariadb.h>
#include <atomic>

namespace dariadb_bench {
// TODO use cmd line params
const size_t total_threads_count = 5;
const size_t iteration_count = 3000000;

void thread_writer_rnd_stor(dariadb::Id id, dariadb::Time sleep_time,
                            std::atomic_long *append_count,
                            dariadb::storage::MeasStorage_ptr ms) {
  auto m = dariadb::Meas::empty();
  m.time = dariadb::timeutil::current_time();
  for (size_t i = 0; i < dariadb_bench::iteration_count; i++) {
    m.id = id;
    m.flag = dariadb::Flag(id);
    m.src = dariadb::Flag(id);
    m.time += sleep_time;
    m.value = dariadb::Value(i);
    ms->append(m);
    (*append_count)++;
    // std::this_thread::sleep_for(sleep_duration);
  }
}
}
