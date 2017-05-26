#include <libdariadb/storage/cursors.h>
#include <benchmark/benchmark_api.h>
#include <chrono>

class Cursors : public benchmark::Fixture {
public:
  dariadb::CursorsList MakeCusrorsList(const ::benchmark::State &st) {
    dariadb::CursorsList cursors;
    auto count = st.range(0);
    auto meases = st.range(1);
    for (int i = 0; i < count; ++i) {
      dariadb::MeasArray ma(meases);
      for (int j = 0; j < meases; ++j) {
        ma[j].time = i + j;
        ma[j].value = i + j;
      }
      auto fr = dariadb::Cursor_Ptr{new dariadb::storage::FullCursor(std::move(ma))};
      cursors.push_back(fr);
    }
    return cursors;
  }
};

BENCHMARK_DEFINE_F(Cursors, Colapse)(benchmark::State &state) {
  while (state.KeepRunning()) {
    auto cursors = MakeCusrorsList(state);
    auto start = std::chrono::high_resolution_clock::now();
    benchmark::DoNotOptimize(
        dariadb::storage::CursorWrapperFactory::colapseCursors(std::move(cursors)));

    auto end = std::chrono::high_resolution_clock::now();

    auto elapsed_seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

    state.SetIterationTime(elapsed_seconds.count());
  }
}
BENCHMARK_REGISTER_F(Cursors, Colapse)
    ->Args({10, 100})
    ->Args({100, 100})
    ->Args({1000, 1000});

BENCHMARK_DEFINE_F(Cursors, MergeReaderCreate)(benchmark::State &state) {
  while (state.KeepRunning()) {
    auto cursors = MakeCusrorsList(state);
    auto start = std::chrono::high_resolution_clock::now();
    benchmark::DoNotOptimize(dariadb::storage::MergeSortCursor{std::move(cursors)});

    auto end = std::chrono::high_resolution_clock::now();

    auto elapsed_seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

    state.SetIterationTime(elapsed_seconds.count());
  }
}

BENCHMARK_REGISTER_F(Cursors, MergeReaderCreate)
    ->Args({10, 100})
    ->Args({100, 100})
    ->Args({1000, 100});

BENCHMARK_DEFINE_F(Cursors, MergeReaderRead)(benchmark::State &state) {

  while (state.KeepRunning()) {
    auto cursors = MakeCusrorsList(state);
    dariadb::storage::MergeSortCursor msr{std::move(cursors)};

    auto start = std::chrono::high_resolution_clock::now();
    while (!msr.is_end()) {
      benchmark::DoNotOptimize(msr.readNext());
    }
    auto end = std::chrono::high_resolution_clock::now();

    auto elapsed_seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

    state.SetIterationTime(elapsed_seconds.count());
  }
}
BENCHMARK_REGISTER_F(Cursors, MergeReaderRead)
    ->Args({10000, 200})
    ->Args({12000, 200})
    ->Args({13000, 200})
    ->Args({14000, 200});