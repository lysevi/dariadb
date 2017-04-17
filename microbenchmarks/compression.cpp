#include <libdariadb/compression/compression.h>
#include <libdariadb/compression/delta.h>
#include <libdariadb/compression/flag.h>
#include <libdariadb/compression/xor.h>

#include <benchmark/benchmark_api.h>

class Compression : public benchmark::Fixture {
  virtual void SetUp(const ::benchmark::State &) {
    buffer = new uint8_t[test_buffer_size];
    std::fill_n(buffer, test_buffer_size, uint8_t());
    size = test_buffer_size;
  }

  virtual void TearDown(const ::benchmark::State &) {
    delete[] buffer;
    size = 0;
  }

public:
  size_t test_buffer_size = 1024 * 1024 * 100;
  uint8_t *buffer;
  size_t size;
};

BENCHMARK_DEFINE_F(Compression, Delta)(benchmark::State &state) {
  dariadb::compression::Range rng{buffer, buffer + size};

  std::vector<dariadb::Time> deltas{50, 2553, 1000, 524277, 500};
  dariadb::Time t = 0;

  while (state.KeepRunning()) {
    size_t packed = 0;
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::DeltaCompressor dc(bw);
    std::fill_n(buffer, test_buffer_size, uint8_t());

    for (int i = 0; i < state.range(0); i++) {
      if (!dc.append(t)) {
        break;
      }
      packed++;
      t += deltas[i % deltas.size()];
      if (t > dariadb::MAX_TIME) {
        t = 0;
      }
    }

    auto w = dc.used_space();
    auto sz = sizeof(dariadb::Time) * packed;
    state.counters["used space"] = ((w * 100.0) / (sz));
  }
}

BENCHMARK_DEFINE_F(Compression, Xor)(benchmark::State &state) {
  dariadb::Value t = 3.14;
  while (state.KeepRunning()) {
    size_t packed = 0;
    dariadb::compression::Range rng{buffer, buffer + size};
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorCompressor dc(bw);
    for (int i = 0; i < state.range(0); i++) {
      if (!dc.append(t)) {
        break;
      }
      t *= 1.5;
      packed++;
    }
    auto w = dc.used_space();
    auto sz = sizeof(dariadb::Value) * packed;
    state.counters["used space"] = ((w * 100.0) / (sz));
  }
}

BENCHMARK_DEFINE_F(Compression, Flag)(benchmark::State &state) {
  dariadb::Flag t = 1;
  while (state.KeepRunning()) {
    dariadb::compression::Range rng{buffer, buffer + size};
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorCompressor dc(bw);
    size_t packed = 0;
    for (int i = 0; i < state.range(0) / 2; i++) {
      if (!dc.append(t)) {
        break;
      }
      packed++;
      if (!dc.append(t)) {
        break;
      }
      packed++;
      t++;
    }
    auto w = dc.used_space();
    auto sz = sizeof(dariadb::Flag) * packed;
    state.counters["used space"] = ((w * 100.0) / (sz));
  }
}

BENCHMARK_DEFINE_F(Compression, Meas)(benchmark::State &state) {

  dariadb::Time t = 0;
  auto m = dariadb::Meas();
  while (state.KeepRunning()) {
    dariadb::compression::Range rng{buffer, buffer + size};
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::CopmressedWriter cwr{bw};
    size_t packed = 0;
    for (int i = 0; i < state.range(0) / 2; i++) {
      m.time = t++;
      m.flag = dariadb::Flag(i);
      m.value = dariadb::Value(i);
      if (!cwr.append(m)) {
        break;
      }
      packed++;
    }
    auto w = cwr.usedSpace();
    auto sz = sizeof(dariadb::Meas) * packed;
    state.counters["used space"] = ((w * 100.0) / (sz));
  }
}
BENCHMARK_REGISTER_F(Compression, Delta)->Arg(100)->Arg(10000);
BENCHMARK_REGISTER_F(Compression, Xor)->Arg(100)->Arg(10000);
BENCHMARK_REGISTER_F(Compression, Flag)->Arg(100)->Arg(10000);
BENCHMARK_REGISTER_F(Compression, Meas)->Arg(100)->Arg(10000);
