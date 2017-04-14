#include <libdariadb/meas.h>
#include <libdariadb/utils/jenkins_hash.h>
#include <libdariadb/utils/striped_map.h>
#include <benchmark/benchmark_api.h>

const size_t Jenkins_SMALL_BENCHMARK_BUFFER = 1000;
const size_t Jenkins_BIG_BENCHMARK_BUFFER = Jenkins_SMALL_BENCHMARK_BUFFER * 10;

class Jenkins : public benchmark::Fixture {
  virtual void SetUp(const ::benchmark::State &st) {
    buffer = new dariadb::Id[st.range(0)];
    size = st.range(0);
  }

  virtual void TearDown() {
    delete[] buffer;
    size = 0;
  }

public:
  dariadb::Id *buffer;
  size_t size;
};

BENCHMARK_DEFINE_F(Jenkins, Hash)(benchmark::State &state) {
  while (state.KeepRunning()) {
    for (size_t i = 0; i < size; ++i) {
      benchmark::DoNotOptimize(dariadb::utils::jenkins_one_at_a_time_hash(buffer[i]));
    }
  }
}
BENCHMARK_REGISTER_F(Jenkins, Hash)->Arg(Jenkins_SMALL_BENCHMARK_BUFFER);
BENCHMARK_REGISTER_F(Jenkins, Hash)->Arg(Jenkins_BIG_BENCHMARK_BUFFER);

class StripedMap : public benchmark::Fixture {
  virtual void SetUp(const ::benchmark::State &) {}

  virtual void TearDown() {}

public:
  dariadb::utils::stripped_map<int, int> target;
};

BENCHMARK_DEFINE_F(StripedMap, Insertion)(benchmark::State &state) {
  while (state.KeepRunning()) {
    for (int i = 0; i < state.range(0); ++i) {
      target.insert(i, i);
    }
  }
}

BENCHMARK_DEFINE_F(StripedMap, Search)(benchmark::State &state) {
  while (state.KeepRunning()) {
    for (int i = 0; i < state.range(0); ++i) {
      benchmark::DoNotOptimize(target.find_bucket(i));
    }
  }
}

BENCHMARK_REGISTER_F(StripedMap, Insertion)->Arg(Jenkins_SMALL_BENCHMARK_BUFFER);
BENCHMARK_REGISTER_F(StripedMap, Insertion)->Arg(Jenkins_BIG_BENCHMARK_BUFFER);
BENCHMARK_REGISTER_F(StripedMap, Search)->Arg(Jenkins_SMALL_BENCHMARK_BUFFER);
BENCHMARK_REGISTER_F(StripedMap, Search)->Arg(Jenkins_BIG_BENCHMARK_BUFFER);