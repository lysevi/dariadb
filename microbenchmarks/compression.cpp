#include <libdariadb/compression/compression.h>
#include <libdariadb/compression/delta.h>
#include <libdariadb/compression/flag.h>
#include <libdariadb/compression/xor.h>

#include <benchmark/benchmark_api.h>

class CompressionDelta : public benchmark::Fixture {
  virtual void SetUp(const ::benchmark::State &st) {
    auto test_buffer_size = 1024 * 1024 * 100;
    uint8_t *buffer = new uint8_t[test_buffer_size];
    size = test_buffer_size;
  }

  virtual void TearDown() {
    delete[] buffer;
    size = 0;
  }
public:
  uint8_t *buffer;
  size_t size;
};

BENCHMARK_DEFINE_F(CompressionDelta, Compress)(benchmark::State &state) {
  dariadb::compression::Range rng{buffer, buffer + size};
  auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
  dariadb::compression::DeltaCompressor dc(bw);

  while (state.KeepRunning()) {
    dc.append(state.range(0));
  }
}
BENCHMARK_REGISTER_F(CompressionDelta, Compress)->Arg(50);
BENCHMARK_REGISTER_F(CompressionDelta, Compress)->Arg(2553);
BENCHMARK_REGISTER_F(CompressionDelta, Compress)->Arg(1000);
BENCHMARK_REGISTER_F(CompressionDelta, Compress)->Arg(2000);
BENCHMARK_REGISTER_F(CompressionDelta, Compress)->Arg(500);
