#include <libdariadb/statistic/calculator.h>
#include <benchmark/benchmark_api.h>
#include <sstream>
#include <cmath>
using namespace dariadb::statistic;

class StatisticFunction : public benchmark::Fixture {
  virtual void SetUp(const ::benchmark::State &st) {
    auto count = st.range(0);
    ma.resize(count);
    for (int i = 0; i < count; ++i) {
      ma[i].id = dariadb::Id(0);
      ma[i].time = i;
      ma[i].value = std::sin(1.0/i);
    }
  }

  virtual void TearDown(const ::benchmark::State &) { ma.clear(); }

public:
  dariadb::MeasArray ma;
};

BENCHMARK_DEFINE_F(StatisticFunction, Average)(benchmark::State &state) {
  while (state.KeepRunning()) {
    auto av = FunctionFactory::make_one("average");
    for (const auto &m : ma) {
      av->apply(m);
      benchmark::DoNotOptimize(av->result());
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
    auto media = FunctionFactory::make_one("median");
    for (const auto &m : ma) {
      media->apply(m);
      benchmark::DoNotOptimize(media->result());
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
      sigma->apply(m);
      benchmark::DoNotOptimize(sigma->result());
    }
  }
}
BENCHMARK_REGISTER_F(StatisticFunction, Sigma)->Arg(10)->Arg(100)->Arg(1000)->Arg(10000);