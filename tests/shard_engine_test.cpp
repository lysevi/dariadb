#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include "test_common.h"
#include <boost/test/unit_test.hpp>

#include <libdariadb/dariadb.h>
#include <libdariadb/engines/engine.h>
#include <libdariadb/engines/shard.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/utils/fs.h>
#include <algorithm>
#include <iostream>

BOOST_AUTO_TEST_CASE(Shard_common_test) {
  const std::string storage_path = "testStorage";
  const std::string storage_path_shard1 = "testStorage_shard1";
  const std::string storage_path_shard2 = "testStorage_shard2";

  const dariadb::Time from = 0;
  const dariadb::Time to = from + 1000;
  const dariadb::Time step = 10;
  const size_t chunk_size = 256;
  using namespace dariadb;
  using namespace dariadb::storage;
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  if (dariadb::utils::fs::path_exists(storage_path_shard1)) {
    dariadb::utils::fs::rm(storage_path_shard1);
  }
  if (dariadb::utils::fs::path_exists(storage_path_shard2)) {
    dariadb::utils::fs::rm(storage_path_shard2);
  }
  {
    auto settings = dariadb::storage::Settings::create(storage_path_shard1);
    settings->wal_cache_size.setValue(100);
    settings->wal_file_size.setValue(settings->wal_cache_size.value() * 5);
    settings->chunk_size.setValue(chunk_size);
    settings->save();
  }

  {
    auto settings = dariadb::storage::Settings::create(storage_path_shard2);
    settings->wal_cache_size.setValue(100);
    settings->wal_file_size.setValue(settings->wal_cache_size.value() * 5);
    settings->chunk_size.setValue(chunk_size);
    settings->save();
  }
  {
    std::cout << "Shard_common_test.\n";

    auto shard_storage = ShardEngine::create(storage_path);
    BOOST_CHECK(shard_storage != nullptr);

    shard_storage->shardAdd({storage_path_shard1, "shard1", {Id(0)}});

    shard_storage->shardAdd({storage_path_shard2, "shard2", IdSet()});
    shard_storage->shardAdd(
        {storage_path_shard1, "shard1", {Id(1), Id(2), Id(3), Id(4)}});
    BOOST_CHECK(shard_storage->settings()->storage_path.value() == storage_path);
  }
  {
    auto shard_storage = dariadb::open_storage(storage_path);
    BOOST_CHECK(shard_storage != nullptr);
    BOOST_CHECK(shard_storage->settings()->storage_path.value() == storage_path);

    auto shard_raw_ptr = dynamic_cast<ShardEngine *>(shard_storage.get());

    auto all_shards = shard_raw_ptr->shardList();
    BOOST_CHECK_EQUAL(all_shards.size(), size_t(2));

    dariadb_test::storage_test_check(shard_storage.get(), from, to, step, true, true,
                                     false);
    shard_storage->fsck();
    shard_storage->repack();
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  if (dariadb::utils::fs::path_exists(storage_path_shard1)) {
    dariadb::utils::fs::rm(storage_path_shard1);
  }
  if (dariadb::utils::fs::path_exists(storage_path_shard2)) {
    dariadb::utils::fs::rm(storage_path_shard2);
  }
}

BOOST_AUTO_TEST_CASE(Shard_common_memory_test) {
  const std::string storage_path = "testStorage";
  const std::string storage_path_shard1 = "testStorage_shard1";
  const std::string storage_path_shard2 = "testStorage_shard2";

  const dariadb::Time from = 0;
  const dariadb::Time to = from + 10000;
  const dariadb::Time step = 10;
  const size_t chunk_size = 256;
  using namespace dariadb;
  using namespace dariadb::storage;
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  if (dariadb::utils::fs::path_exists(storage_path_shard1)) {
    dariadb::utils::fs::rm(storage_path_shard1);
  }
  if (dariadb::utils::fs::path_exists(storage_path_shard2)) {
    dariadb::utils::fs::rm(storage_path_shard2);
  }
  {
    auto settings = dariadb::storage::Settings::create(storage_path_shard1);
    settings->strategy.setValue(dariadb::STRATEGY::MEMORY);
    settings->memory_limit.setValue(10 * 1024 * 128);
    settings->chunk_size.setValue(chunk_size);
    settings->save();
  }

  {
    auto settings = dariadb::storage::Settings::create(storage_path_shard2);
    settings->strategy.setValue(dariadb::STRATEGY::MEMORY);
    settings->memory_limit.setValue(10 * 1024 * 128);
    settings->chunk_size.setValue(chunk_size);
    settings->save();
  }
  {
    std::cout << "Shard_common_memory_test.\n";

    auto shard_storage = ShardEngine::create(storage_path);
    BOOST_CHECK(shard_storage != nullptr);

    shard_storage->shardAdd({storage_path_shard1, "shard1", {Id(0)}});

    shard_storage->shardAdd({storage_path_shard2, "shard2", IdSet()});
    shard_storage->shardAdd(
        {storage_path_shard1, "shard1", {Id(1), Id(2), Id(3), Id(4)}});
    BOOST_CHECK(shard_storage->settings()->storage_path.value() == storage_path);
  }
  {
    auto shard_storage = dariadb::open_storage(storage_path);
    BOOST_CHECK(shard_storage != nullptr);
    BOOST_CHECK(shard_storage->settings()->storage_path.value() == storage_path);

    auto shard_raw_ptr = dynamic_cast<ShardEngine *>(shard_storage.get());

    auto all_shards = shard_raw_ptr->shardList();
    BOOST_CHECK_EQUAL(all_shards.size(), size_t(2));

    dariadb_test::storage_test_check(shard_storage.get(), from, to, step, true, false,
                                     false);
    shard_storage->fsck();
    shard_storage->repack();
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  if (dariadb::utils::fs::path_exists(storage_path_shard1)) {
    dariadb::utils::fs::rm(storage_path_shard1);
  }
  if (dariadb::utils::fs::path_exists(storage_path_shard2)) {
    dariadb::utils::fs::rm(storage_path_shard2);
  }
}
