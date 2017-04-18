#include <libdariadb/statistic/calculator.h>
#include <benchmark/benchmark_api.h>

class StatisticFunction : public benchmark::Fixture {
  virtual void SetUp(const ::benchmark::State &st) {
    auto count = st.range(0);
    ma.resize(count);
    for (int i = 0; i < count; ++i) {
      ma[i].id = dariadb::Id(0);
      ma[i].time = i;
      ma[i].value = i;
    }
  }

  virtual void TearDown(const ::benchmark::State &) { ma.clear(); }

public:
  dariadb::MeasArray ma;
};

BENCHMARK_DEFINE_F(StatisticFunction, Average)(benchmark::State &state) {
  while (state.KeepRunning()) {
    dariadb::statistic::Average av;
    for (const auto &m : ma) {
      av.apply(m);
      benchmark::DoNotOptimize(av.result());
    }
  }
}
BENCHMARK_REGISTER_F(StatisticFunction, Average)
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(10000);
