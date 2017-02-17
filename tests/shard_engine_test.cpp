#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include "test_common.h"
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <iostream>
#include <libdariadb/shard.h>
#include <libdariadb/utils/fs.h>

BOOST_AUTO_TEST_CASE(Shard_common_test) {
  const std::string storage_path = "testStorage";
  const std::string storage_path_shard1 = "testStorage_shard1";
  const std::string storage_path_shard2 = "testStorage_shard2";
  
  /* const dariadb::Time from = 0;
   const dariadb::Time to = from + 1000;
   const dariadb::Time step = 10;*/
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
    std::cout << "Shard_common_test.\n";

    auto shard_storage = ShardEngine::create(storage_path);
    BOOST_CHECK(shard_storage != nullptr);

    shard_storage->shardAdd(
        {storage_path_shard1, "shard1", {Id(0), Id(1), Id(2), Id(3), Id(4)}});
    shard_storage->shardAdd({storage_path_shard2, "shard2", IdSet()});
    
  }
  {
	  auto shard_storage = ShardEngine::create(storage_path);
	  BOOST_CHECK(shard_storage != nullptr);
	  
	  auto all_shards = shard_storage->shardList();
	  BOOST_CHECK_EQUAL(all_shards.size(), size_t(2));
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
