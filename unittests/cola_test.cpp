
#include <catch.hpp>

#include "helpers.h"

#include <iostream>

#include <libdariadb/storage/cola.h>
#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/utils.h>

TEST_CASE("COLA.IndexreadWrite") {
  using namespace dariadb::storage;
  Cola::Param p{uint8_t(3), uint8_t(5)};
  auto sz = Cola::index_size(p);
  EXPECT_GT(sz, size_t(0));

  uint8_t *buffer = new uint8_t[sz + 1];
  buffer[sz] = uint8_t(255);
  dariadb::Id targetId{10};

  uint64_t address = 1, chunk_id = 1;
  size_t addeded = 0;
  dariadb::Time maxTime = 1;
  {
    Cola c(p, targetId, buffer);
    EXPECT_EQ(buffer[sz], uint8_t(255));
    EXPECT_EQ(c.levels(), p.levels);
    EXPECT_EQ(c.targetId(), targetId);

    while (c.addLink(address, chunk_id, maxTime)) {
      addeded++;
     /* auto queryResult = c.queryLink(dariadb::Time(1), maxTime);
      EXPECT_EQ(addeded, queryResult.size());*/
	  EXPECT_EQ(buffer[sz], uint8_t(255));
      address++;
      chunk_id++;
      maxTime++;
    }
    /*EXPECT_GT(addeded, size_t(0));
    auto queryResult = c.queryLink(dariadb::Time(1), maxTime);
    EXPECT_EQ(addeded, queryResult.size());*/
  }
 /* {
    Cola c(buffer);
    EXPECT_EQ(c.levels(), p.levels);
    EXPECT_EQ(c.targetId(), targetId);
    auto queryResult = c.queryLink(dariadb::Time(1), maxTime);
    EXPECT_EQ(addeded, queryResult.size());
  }*/
  delete[] buffer;
}

/*
TEST_CASE("COLA", "Init") {
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
*/