#include <gtest/gtest.h>

#include "helpers.h"

#include <iostream>

#include <libdariadb/storage/cola.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/utils.h>
#include <libdariadb/utils/async/thread_manager.h>


TEST(COLA, Init) {
  const std::string storagePath = "testStorage";
  const size_t chunks_size = 200;

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  dariadb_test::MeasesList addeded;

  auto settings = dariadb::storage::Settings::create(storagePath);
  settings->chunk_size.setValue(chunks_size);

  auto manifest = dariadb::storage::Manifest::create(settings);

  auto _engine_env = dariadb::storage::EngineEnvironment::create();
  _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
                           settings.get());
  _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::MANIFEST,
                           manifest.get());

  dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());


  manifest = nullptr;
  dariadb::utils::async::ThreadManager::stop();

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}

