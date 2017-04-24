#include <libdariadb/timeutil.h>
#include <benchmark/benchmark_api.h>

static void Time_to_datetime(benchmark::State &state) {
  auto t = dariadb::timeutil::current_time();
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(dariadb::timeutil::to_datetime(t));
  }
}
BENCHMARK(Time_to_datetime);

static void Time_to_string(benchmark::State &state) {
  auto t = dariadb::timeutil::current_time();
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(dariadb::timeutil::to_string(t));
  }
}
BENCHMARK(Time_to_string);

static void Time_from_string(benchmark::State &state) {
  auto t = dariadb::timeutil::current_time();
  auto str = dariadb::timeutil::to_string(t);
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(dariadb::timeutil::from_string(str));
  }
}
BENCHMARK(Time_from_string);