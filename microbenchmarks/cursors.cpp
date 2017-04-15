#include <libdariadb/storage/cursors.h>
#include <benchmark/benchmark_api.h>

class Cursors : public benchmark::Fixture {
  virtual void SetUp(const ::benchmark::State &st) {
    auto count = st.range(0);
    auto meases = st.range(1);
    for (int i = 0; i < count; ++i) {
      dariadb::MeasArray ma(meases);
      for (int j = 0; j < meases; ++j) {
        ma[j].time = i + j;
        ma[j].value = i + j;
      }
      auto fr = dariadb::Cursor_Ptr{new dariadb::storage::FullCursor(ma)};
      cursors.push_back(fr);
    }
  }

  virtual void TearDown(const ::benchmark::State &) { cursors.clear(); }

public:
  dariadb::CursorsList cursors;
};

BENCHMARK_DEFINE_F(Cursors, Colapse)(benchmark::State &state) {
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(
        dariadb::storage::CursorWrapperFactory::colapseCursors(cursors));
  }
}
BENCHMARK_REGISTER_F(Cursors, Colapse)->Args({10, 100})->Args({100, 100 })->Args({1000, 1000});

BENCHMARK_DEFINE_F(Cursors, MergeReaderCreate)(benchmark::State &state) {
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(dariadb::storage::MergeSortCursor{cursors});
  }
}
BENCHMARK_REGISTER_F(Cursors, MergeReaderCreate)
    ->Args({10, 100})
    ->Args({100, 100})
    ->Args({1000, 100});
