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

BENCHMARK_DEFINE_F(Compression, DeltaPack)(benchmark::State &state) {
  dariadb::compression::Range rng{buffer, buffer + size};

  std::vector<dariadb::Time> deltas{50, 2553, 1000, 524277, 500};
  dariadb::Time t = 0;
  auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
  while (state.KeepRunning()) {
    size_t packed = 0;
    bw->reset_pos();
    std::fill_n(buffer, test_buffer_size, uint8_t());

    dariadb::compression::DeltaCompressor dc(bw);

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

BENCHMARK_DEFINE_F(Compression, DeltaUnpack)(benchmark::State &state) {
  dariadb::compression::Range rng{buffer, buffer + size};

  std::vector<dariadb::Time> deltas{50, 2553, 1000, 524277, 500};
  dariadb::Time t = 0;
  size_t packed = 0;
  {
    std::fill_n(buffer, test_buffer_size, uint8_t());
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
  }

  while (state.KeepRunning()) {
    auto unpack_bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::DeltaDeCompressor decompress(unpack_bw, 0);
    for (size_t i = 1; i < packed; i++) {
      decompress.read();
    }
  }
}

BENCHMARK_DEFINE_F(Compression, XorPack)(benchmark::State &state) {
  dariadb::Value t = 3.14;
  dariadb::compression::Range rng{buffer, buffer + size};
  auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
  while (state.KeepRunning()) {
    size_t packed = 0;
    bw->reset_pos();
    std::fill_n(buffer, test_buffer_size, uint8_t());

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

BENCHMARK_DEFINE_F(Compression, XorUnpack)(benchmark::State &state) {
  dariadb::Value t = 3.14;
  size_t packed = 0;
  dariadb::compression::Range rng{buffer, buffer + size};
  {
    std::fill_n(buffer, test_buffer_size, uint8_t());
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorCompressor dc(bw);
    for (int i = 0; i < state.range(0); i++) {
      if (!dc.append(t)) {
        break;
      }
      t *= 1.5;
      packed++;
    }
  }

  while (state.KeepRunning()) {
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorDeCompressor dc(bw, 0);
    for (size_t i = 1; i < packed; i++) {
      dc.read();
    }
  }
}

BENCHMARK_DEFINE_F(Compression, FlagPack)(benchmark::State &state) {
  dariadb::Flag t = 1;
  dariadb::compression::Range rng{buffer, buffer + size};
  auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
  while (state.KeepRunning()) {
    bw->reset_pos();
    std::fill_n(buffer, test_buffer_size, uint8_t());

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

BENCHMARK_DEFINE_F(Compression, FlagUnpack)(benchmark::State &state) {
  dariadb::Flag t = 1;
  dariadb::compression::Range rng{buffer, buffer + size};
  size_t packed = 0;
  {
    std::fill_n(buffer, test_buffer_size, uint8_t());
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorCompressor dc(bw);

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
  }
  while (state.KeepRunning()) {
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::FlagDeCompressor dc(bw, 0);

    dariadb::utils::ElapsedTime et;
    for (size_t i = 1; i < packed; i++) {
      dc.read();
    }
  }
}

BENCHMARK_DEFINE_F(Compression, MeasPack)(benchmark::State &state) {

  dariadb::Time t = 0;
  auto m = dariadb::Meas();
  dariadb::compression::Range rng{buffer, buffer + size};
  auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
  while (state.KeepRunning()) {
    bw->reset_pos();
    std::fill_n(buffer, test_buffer_size, uint8_t());
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

BENCHMARK_DEFINE_F(Compression, MeasUnpack)(benchmark::State &state) {
  dariadb::Time t = 0;
  auto m = dariadb::Meas();
  dariadb::compression::Range rng{buffer, buffer + size};
  size_t packed = 0;
  {
    std::fill_n(buffer, test_buffer_size, uint8_t());
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::CopmressedWriter cwr{bw};
    for (int i = 0; i < state.range(0) / 2; i++) {
      m.time = t++;
      m.flag = dariadb::Flag(i);
      m.value = dariadb::Value(i);
      if (!cwr.append(m)) {
        break;
      }
      packed++;
    }
  }

  while (state.KeepRunning()) {
    auto rbw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::CopmressedReader crr{rbw, m};
    for (size_t i = 1; i < packed; i++) {
      crr.read();
    }
  }
}

BENCHMARK_REGISTER_F(Compression, DeltaPack)->Arg(100)->Arg(10000);
BENCHMARK_REGISTER_F(Compression, DeltaUnpack)->Arg(100)->Arg(10000);

BENCHMARK_REGISTER_F(Compression, XorPack)->Arg(100)->Arg(10000);
BENCHMARK_REGISTER_F(Compression, XorUnpack)->Arg(100)->Arg(10000);

BENCHMARK_REGISTER_F(Compression, FlagPack)->Arg(100)->Arg(10000);
BENCHMARK_REGISTER_F(Compression, FlagUnpack)->Arg(100)->Arg(10000);

BENCHMARK_REGISTER_F(Compression, MeasPack)->Arg(100)->Arg(10000);
BENCHMARK_REGISTER_F(Compression, MeasUnpack)->Arg(100)->Arg(10000);
