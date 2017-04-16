#include "../network/common/net_data.h"
#include <benchmark/benchmark_api.h>

class NetworkPack : public benchmark::Fixture {
  virtual void SetUp(const ::benchmark::State &st) {
    auto count = st.range(0);
    auto meases = st.range(1);
    ma.resize(count);
    dariadb::Id id = 1;
    for (size_t i = 1; i < ma.size(); ++i) {
      ma[i].flag = dariadb::Flag(i);
      ma[i].value = dariadb::Value(i);
      ma[i].time = i;
      ma[i].id = id;
      if (i % meases == 0) {
        ++id;
      }
    }
  }

  virtual void TearDown(const ::benchmark::State &) { ma.clear(); }

public:
  dariadb::MeasArray ma;
};

BENCHMARK_DEFINE_F(NetworkPack, Pack)(benchmark::State &state) {
  using dariadb::net::QueryAppend_header;
  using dariadb::net::NetData;

  NetData nd;
  auto hdr = reinterpret_cast<QueryAppend_header *>(&nd.data);
  size_t sz = 0;
  size_t count_processed = 0;
  while (state.KeepRunning()) {
    sz = 0;
    count_processed = QueryAppend_header::make_query(hdr, ma.data(), ma.size(), 0, &sz);
  }
  state.counters["packed"] = ((count_processed * 100.0) / (ma.size()));
}
BENCHMARK_REGISTER_F(NetworkPack, Pack)
    ->Args({100, 1})
    ->Args({100, 2})
    ->Args({100, 100})
    ->Args({1000, 1})
    ->Args({1000, 2})
    ->Args({1000, 1000})
    ->Args({10000, 1000})
    ->Args({10000, 2000})
    ->Args({10000, 5000});
