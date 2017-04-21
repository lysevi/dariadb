#include <libdariadb/dariadb.h>
#include <benchmark/benchmark_api.h>
#include <cmath>
#include <sstream>

#include "common.h"

using namespace dariadb::statistic;

class StatisticFunction : public benchmark::Fixture {
  virtual void SetUp(const ::benchmark::State &st) {
    auto count = st.range(0);
    ma.resize(count);
    for (int i = 0; i < count; ++i) {
      ma[i].id = dariadb::Id(0);
      ma[i].time = i;
      ma[i].value = std::sin(1.0 / (i + 1));
    }
  }

  virtual void TearDown(const ::benchmark::State &) { ma.clear(); }

public:
  dariadb::MeasArray ma;
};

class StatisticCalculation : public benchmark::Fixture {
  virtual void SetUp(const ::benchmark::State &st) {
    microbenchmark_common::replace_std_logger();
    storage = dariadb::memory_only_storage();
    auto count = st.range(0);
    for (int i = 0; i < count; ++i) {
      dariadb::Meas m;
      m.id = dariadb::Id(0);
      m.time = i;
      m.value = std::sin(1.0 / (i + 1));
      storage->append(m);
    }
  }

  virtual void TearDown(const ::benchmark::State &) { storage = nullptr; }

public:
  dariadb::IEngine_Ptr storage;
};
BENCHMARK_DEFINE_F(StatisticFunction, Average)(benchmark::State &state) {
  while (state.KeepRunning()) {
    auto av = FunctionFactory::make_one("average");
    for (const auto &m : ma) {
      benchmark::DoNotOptimize(av->apply(ma));
    }
  }
}
BENCHMARK_REGISTER_F(StatisticFunction, Average)
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(10000);

BENCHMARK_DEFINE_F(StatisticFunction, Percentile)(benchmark::State &state) {
  while (state.KeepRunning()) {
    auto median = FunctionFactory::make_one("median");
    for (const auto &m : ma) {
		benchmark::DoNotOptimize(median->apply(ma));
    }
  }
}
BENCHMARK_REGISTER_F(StatisticFunction, Percentile)
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(10000);

BENCHMARK_DEFINE_F(StatisticFunction, Sigma)(benchmark::State &state) {
  while (state.KeepRunning()) {
    auto sigma = FunctionFactory::make_one("sigma");
    for (const auto &m : ma) {
		benchmark::DoNotOptimize(sigma->apply(ma));
    }
  }
}
BENCHMARK_REGISTER_F(StatisticFunction, Sigma)->Arg(10)->Arg(100)->Arg(1000)->Arg(10000);

BENCHMARK_DEFINE_F(StatisticCalculation, Calculation)(benchmark::State &state) {
  dariadb::statistic::Calculator calc(storage);
  while (state.KeepRunning()) {
    auto all_functions = dariadb::statistic::FunctionFactory::functions();
    auto result = calc.apply(dariadb::Id(10), dariadb::Time(0), dariadb::MAX_TIME,
                             dariadb::Flag(), all_functions);
    benchmark::DoNotOptimize(result);
  }
}
BENCHMARK_REGISTER_F(StatisticCalculation, Calculation)
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(10000);