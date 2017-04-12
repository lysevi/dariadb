#include <libdariadb/utils/crc.h>
#include <benchmark/benchmark_api.h>

const size_t CRC_SMALL_BENCHMARK_BUFFER = 1024;
const size_t CRC_BIG_BENCHMARK_BUFFER = CRC_SMALL_BENCHMARK_BUFFER * 10;

class CrcFixture : public benchmark::Fixture {
  virtual void SetUp(const ::benchmark::State &st) {
    buffer = new char[st.range_x()];
    size = st.range_x();
  }

  virtual void TearDown() {
    delete[] buffer;
    size = 0;
  }

public:
  char *buffer;
  size_t size;
};

BENCHMARK_DEFINE_F(CrcFixture, BufferParam)(benchmark::State &st) {
  while (st.KeepRunning()) {
    dariadb::utils::crc32(buffer, size);
  }
}

BENCHMARK_REGISTER_F(CrcFixture, BufferParam)->Arg(CRC_SMALL_BENCHMARK_BUFFER);
BENCHMARK_REGISTER_F(CrcFixture, BufferParam)->Arg(CRC_BIG_BENCHMARK_BUFFER);