#include <gtest/gtest.h>

#include "helpers.h"

#include <libdariadb/dariadb.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/fs.h>
#include <algorithm>
#include <iostream>

class BenchCallback : public dariadb::IReadCallback {
public:
  BenchCallback() { count = 0; }
  void call(const dariadb::Meas &) { count++; }
  size_t count;
};

void test_statistic_on_engine(dariadb::IEngine_Ptr &storage) {
  dariadb::statistic::Calculator calc(storage);
  auto all_functions = dariadb::statistic::FunctionFactory::functions();
  auto result = calc.apply(dariadb::Id(0), dariadb::Time(0), dariadb::MAX_TIME,
                           dariadb::Flag(), all_functions);

  EXPECT_EQ(result.size(), all_functions.size());
  for (size_t i = 0; i < all_functions.size() - 1; ++i) {
    auto m = result[i];
    EXPECT_TRUE(m.value != dariadb::Value());
  }
}

TEST(Engine, Common_test) {
  const std::string storage_path = "testStorage";
  const size_t chunk_size = 256;

  const dariadb::Time from = 0;
  const dariadb::Time to = from + 1000;
  const dariadb::Time step = 10;
  using namespace dariadb;
  using namespace dariadb::storage;
  {
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    auto settings = dariadb::storage::Settings::create(storage_path);
    settings->wal_cache_size.setValue(size_t(90));
    settings->wal_file_size.setValue(size_t(80));
    settings->chunk_size.setValue(chunk_size);
    dariadb::IEngine_Ptr ms{new Engine(settings)};

    dariadb_test::storage_test_check(ms.get(), from, to, step, true, true, false);

    test_statistic_on_engine(ms);

    auto pages_count = ms->description().pages_count;
    EXPECT_GE(pages_count, size_t(2));
    EXPECT_TRUE(ms->settings() != nullptr);
    EXPECT_TRUE(ms->settings()->storage_path.value() == storage_path);
  }
  {
    { // bad idea remove files from working storage.
      auto ms = dariadb::open_storage(storage_path);
      auto settings = ms->settings();

      auto path_to_raw = settings->raw_path.value();
      settings = nullptr;
      ms->stop();

      ms = nullptr;
      auto index_files = dariadb::utils::fs::ls(path_to_raw, ".pagei");
      EXPECT_TRUE(!index_files.empty());
      for (auto &f : index_files) {
        dariadb::utils::fs::rm(f);
      }
      index_files = dariadb::utils::fs::ls(path_to_raw, ".pagei");
      EXPECT_TRUE(index_files.empty());
    }

    auto ms = dariadb::open_storage(storage_path);
    auto settings = ms->settings();

    ms->fsck();

    ms->wait_all_asyncs();

    // check first id, because that Id placed in compressed pages.
    auto values = ms->readInterval(QueryInterval({dariadb::Id(0)}, 0, from, to));
    EXPECT_EQ(values.size(), dariadb_test::copies_count);

    auto current = ms->currentValue(dariadb::IdArray{}, 0);
    EXPECT_TRUE(current.size() != size_t(0));

    dariadb::logger_info("erase old files");
    ms->settings()->max_store_period.setValue(1);
    while (true) {
      auto index_files = dariadb::utils::fs::ls(settings->raw_path.value(), ".pagei");
      if (index_files.empty()) {
        break;
      }
      dariadb::logger_info("file left:");
      for (auto i : index_files) {
        dariadb::logger_info(i);
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

TEST(Engine, compress_all_test) {
  const std::string storage_path = "testStorage";
  const size_t chunk_size = 256;

  const dariadb::Time from = 0;
  const dariadb::Time to = from + 50;
  const dariadb::Time step = 10;

  using namespace dariadb;
  using namespace dariadb::storage;

  {
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    auto settings = dariadb::storage::Settings::create(storage_path);
    settings->wal_cache_size.setValue(100);
    settings->wal_file_size.setValue(settings->wal_cache_size.value() * 2);
    settings->chunk_size.setValue(chunk_size);
    settings->strategy.setValue(dariadb::STRATEGY::WAL);
    std::unique_ptr<Engine> ms{new Engine(settings)};

    dariadb::IdSet all_ids;
    dariadb::Time maxWritedTime;
    dariadb_test::fill_storage_for_test(ms.get(), from, to, step, &all_ids,
                                        &maxWritedTime, false);

    ms->compress_all();
    while (true) {
      auto wals_count = ms->description().wal_count;
      if (wals_count == 0) {
        break;
      }
      dariadb::utils::sleep_mls(500);
    }
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

TEST(Engine, Subscribe) {
  const std::string storage_path = "testStorage";

  {
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    auto settings = dariadb::storage::Settings::create(storage_path);

    dariadb::IEngine_Ptr ms = std::make_shared<dariadb::Engine>(settings);

    dariadb_test::subscribe_test(ms.get());
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

TEST(Engine, MemStorage_common_test) {
  const std::string storage_path = "testStorage";
  const size_t chunk_size = 128;

  const dariadb::Time from = 0;
  const dariadb::Time to = from + 1000;
  const dariadb::Time step = 10;

  using namespace dariadb;
  using namespace dariadb::storage;

  {
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    auto settings = dariadb::storage::Settings::create(storage_path);
    settings->strategy.setValue(STRATEGY::MEMORY);
    settings->chunk_size.setValue(chunk_size);
    settings->max_chunks_per_page.setValue(5);
    settings->memory_limit.setValue(50 * 1024);
    dariadb::IEngine_Ptr ms{new Engine(settings)};

    dariadb_test::storage_test_check(ms.get(), from, to, step, true, false, false);
    test_statistic_on_engine(ms);

    auto pages_count = ms->description().pages_count;
    EXPECT_GE(pages_count, size_t(2));
    ms->settings()->max_store_period.setValue(1);
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

TEST(Engine, MemOnlyStorage_common_test) {
  const size_t chunk_size = 128;

  const dariadb::Time from = 0;
  const dariadb::Time to = from + 1000;
  const dariadb::Time step = 10;

  using namespace dariadb;
  using namespace dariadb::storage;

  {
    auto settings = dariadb::storage::Settings::create();
    settings->chunk_size.setValue(chunk_size);
    dariadb::IEngine_Ptr ms{new Engine(settings)};

    dariadb_test::storage_test_check(ms.get(), from, to, step, true, false, false);

    test_statistic_on_engine(ms);

    auto pages_count = ms->description().pages_count;
    EXPECT_EQ(pages_count, size_t(0));
    ms->settings()->max_store_period.setValue(1);
    while (true) {
      dariadb::QueryInterval qi({dariadb::Id(0)}, dariadb::Flag(), from, to);
      auto values = ms->readInterval(qi);
      if (values.empty()) {
        break;
      } else {
        dariadb::logger_info("values !empty() ", values.size());

        dariadb::utils::sleep_mls(500);
      }
    }
  }
}

TEST(Engine, Cache_common_test) {
  const std::string storage_path = "testStorage";

  const size_t chunk_size = 128;
  const dariadb::Time from = 0;
  const dariadb::Time to = from + 1000;
  const dariadb::Time step = 10;

  using namespace dariadb;
  using namespace dariadb::storage;

  {
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    auto settings = dariadb::storage::Settings::create(storage_path);
    settings->strategy.setValue(STRATEGY::CACHE);
    settings->chunk_size.setValue(chunk_size);
    settings->memory_limit.setValue(50 * 1024);
    settings->wal_file_size.setValue(50);
	settings->wal_cache_size.setValue(70);
    dariadb::IEngine_Ptr ms{new Engine(settings)};

    dariadb_test::storage_test_check(ms.get(), from, to, step, true, true, false);

    test_statistic_on_engine(ms);
    auto descr = ms->description();
    EXPECT_GT(descr.pages_count, size_t(0));
    ms->settings()->max_store_period.setValue(1);
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}
