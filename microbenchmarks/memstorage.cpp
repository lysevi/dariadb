#include <libdariadb/storage/memstorage/memstorage.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/utils/async/thread_manager.h>


#include <benchmark/benchmark_api.h>
#include "common.h"


class Memstorage : public benchmark::Fixture {
  virtual void SetUp(const ::benchmark::State &) {
	  microbenchmark_common::replace_std_logger();
    settings = dariadb::storage::Settings::create();
    settings->chunk_size.setValue(10);
    auto _engine_env = dariadb::storage::EngineEnvironment::create();
    _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
                             settings.get());
    dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

    ms = dariadb::storage::MemStorage::create(_engine_env, size_t(0));
  }

  virtual void TearDown(const ::benchmark::State &) {
    ms = nullptr;
    dariadb::utils::async::ThreadManager::stop();
  }

public:
  dariadb::storage::Settings_ptr settings;
  dariadb::storage::MemStorage_ptr ms;
};

BENCHMARK_DEFINE_F(Memstorage, AddNewTrack)(benchmark::State &state) {
  auto meas = dariadb::Meas();
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(ms->append(meas));
    meas.id++;
  }
}
BENCHMARK_REGISTER_F(Memstorage, AddNewTrack)
    ->Arg(1000)
    ->Arg(10000)
    ->Arg(20000)
    ->Arg(50000);
