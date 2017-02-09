#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include "test_common.h"
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <iostream>
#include <libdariadb/engine.h>
#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/fs.h>

class BenchCallback : public dariadb::IReadCallback {
public:
  BenchCallback() { count = 0; }
  void call(const dariadb::Meas &) { count++; }
  size_t count;
};

BOOST_AUTO_TEST_CASE(Engine_common_test) {
  const std::string storage_path = "testStorage";
  const size_t chunk_size = 256;

  const dariadb::Time from = 0;
  const dariadb::Time to = from + 1000;
  const dariadb::Time step = 10;
  using namespace dariadb;
  using namespace dariadb::storage;
  {
    std::cout << "Engine_common_test.\n";
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    auto settings = dariadb::storage::Settings::create(storage_path);
    settings->wal_cache_size.setValue(100);
    settings->wal_file_size.setValue(settings->wal_cache_size.value() * 5);
    settings->chunk_size.setValue(chunk_size);
    std::unique_ptr<Engine> ms{new Engine(settings)};

    dariadb_test::storage_test_check(ms.get(), from, to, step, true, true);

    auto pages_count = ms->description().pages_count;
    BOOST_CHECK_GE(pages_count, size_t(2));
  }
  {
    std::cout << "reopen closed storage\n";
    auto settings = dariadb::storage::Settings::create(storage_path);

    auto manifest = dariadb::storage::Manifest::create(settings);

    auto manifest_version = manifest->get_format();

    manifest = nullptr;

    auto raw_ptr = new Engine(settings);

    dariadb::IMeasStorage_ptr ms{raw_ptr};
    auto index_files = dariadb::utils::fs::ls(settings->raw_path.value(), ".pagei");
	BOOST_CHECK(!index_files.empty());
	for (auto &f : index_files) {
      dariadb::utils::fs::rm(f);
    }
	index_files = dariadb::utils::fs::ls(settings->raw_path.value(), ".pagei");
	BOOST_CHECK(index_files.empty());
    raw_ptr->fsck();

    // check first id, because that Id placed in compressed pages.
    auto values =
        ms->readInterval(QueryInterval({dariadb::Id(0)}, 0, from, to));
    BOOST_CHECK_EQUAL(values.size(), dariadb_test::copies_count);

    auto current = ms->currentValue(dariadb::IdArray{}, 0);
    BOOST_CHECK(current.size() != size_t(0));
  }
  std::cout << "end\n";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(Engine_compress_all_test) {
  const std::string storage_path = "testStorage";
  const size_t chunk_size = 256;

  const dariadb::Time from = 0;
  const dariadb::Time to = from + 50;
  const dariadb::Time step = 10;

  using namespace dariadb;
  using namespace dariadb::storage;

  {
    std::cout << "Engine_compress_all_test\n";
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

    auto pages_count = ms->description().pages_count;
    auto wals_count = ms->description().wal_count;
    BOOST_CHECK_GE(pages_count, size_t(1));
    BOOST_CHECK_EQUAL(wals_count, size_t(0));
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

class Moc_SubscribeClbk : public dariadb::IReadCallback {
public:
  std::list<dariadb::Meas> values;
  void apply(const dariadb::Meas &m) override { values.push_back(m); }
  void is_end() override {}
  ~Moc_SubscribeClbk() {}
};

BOOST_AUTO_TEST_CASE(Subscribe) {
  const size_t id_count = 5;
  auto c1 = std::make_shared<Moc_SubscribeClbk>();
  auto c2 = std::make_shared<Moc_SubscribeClbk>();
  auto c3 = std::make_shared<Moc_SubscribeClbk>();
  auto c4 = std::make_shared<Moc_SubscribeClbk>();

  const std::string storage_path = "testStorage";
  const size_t chunk_size = 256;

  {
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    auto settings = dariadb::storage::Settings::create(storage_path);
    settings->wal_cache_size.setValue(chunk_size);
    settings->chunk_size.setValue(chunk_size);

    auto ms = std::make_shared<dariadb::Engine>(settings);

    dariadb::IdArray ids{};
    ms->subscribe(ids, 0, c1); // all
    ids.push_back(2);
    ms->subscribe(ids, 0, c2); // 2
    ids.push_back(1);
    ms->subscribe(ids, 0, c3); // 1 2
    ids.clear();
    ms->subscribe(ids, 1, c4); // with flag=1

    auto m = dariadb::Meas();
    const size_t total_count = 100;
    const dariadb::Time time_step = 1;

    for (size_t i = 0; i < total_count; i += time_step) {
      m.id = i % id_count;
      m.flag = dariadb::Flag(i);
      m.time = i;
      m.value = 0;
      ms->append(m);
    }
    BOOST_CHECK_EQUAL(c1->values.size(), total_count);

    BOOST_CHECK_EQUAL(c2->values.size(), size_t(total_count / id_count));
    BOOST_CHECK_EQUAL(c2->values.front().id, dariadb::Id(2));

    BOOST_CHECK_EQUAL(c3->values.size(), size_t(total_count / id_count) * 2);
    BOOST_CHECK_EQUAL(c3->values.front().id, dariadb::Id(1));

    BOOST_CHECK_EQUAL(c4->values.size(), size_t(1));
    BOOST_CHECK_EQUAL(c4->values.front().flag, dariadb::Flag(1));
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(Engine_MemStorage_common_test) {
  const std::string storage_path = "testStorage";
  const size_t chunk_size = 256;

  const dariadb::Time from = 0;
  const dariadb::Time to = from + 1000;
  const dariadb::Time step = 10;

  using namespace dariadb;
  using namespace dariadb::storage;

  {
    std::cout << "Engine_MemStorage_common_test\n";
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    auto settings = dariadb::storage::Settings::create(storage_path);
    settings->strategy.setValue(STRATEGY::MEMORY);
    settings->chunk_size.setValue(chunk_size);
    settings->chunk_size.setValue(128);
    settings->memory_limit.setValue(50 * 1024);
    std::unique_ptr<Engine> ms{new Engine(settings)};

    dariadb_test::storage_test_check(ms.get(), from, to, step, true, false);

    auto pages_count = ms->description().pages_count;
    BOOST_CHECK_GE(pages_count, size_t(2));
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(Engine_Cache_common_test) {
  const std::string storage_path = "testStorage";

  const size_t chunk_size = 128;
  const dariadb::Time from = 0;
  const dariadb::Time to = from + 1000;
  const dariadb::Time step = 10;

  using namespace dariadb;
  using namespace dariadb::storage;

  {
    std::cout << "Engine_Cache_common_test\n";
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    auto settings = dariadb::storage::Settings::create(storage_path);
    settings->strategy.setValue(STRATEGY::CACHE);
    settings->chunk_size.setValue(chunk_size);
    settings->memory_limit.setValue(50 * 1024);
    settings->wal_file_size.setValue(2000);
    std::unique_ptr<Engine> ms{new Engine(settings)};

    dariadb_test::storage_test_check(ms.get(), from, to, step, true, true);

    auto descr = ms->description();
    BOOST_CHECK_GT(descr.pages_count, size_t(0));
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(Engine_join_test) {
  const std::string storage_path = "testStorage";
  const size_t chunk_size = 256;

  const dariadb::Time from = 0;
  const dariadb::Time to = from + 50;
  const dariadb::Time step = 10;

  using namespace dariadb;
  using namespace dariadb::storage;

  {
    std::cout << "Engine_join_test\n";
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

    const dariadb::Id no_exists_id = 77676;
    std::list<dariadb::QueryInterval> qs;
    qs.push_back(dariadb::QueryInterval({0}, 0, 0, 10));
    qs.push_back(dariadb::QueryInterval({1}, 0, 10, 20));
    qs.push_back(dariadb::QueryInterval({2, 3}, 0, 20, to));
    qs.push_back(dariadb::QueryInterval({no_exists_id}, 0, 20, to));

    auto tm = std::make_unique<dariadb::storage::Join::TableMaker>();
    ms->join(qs, tm.get());
    for (auto row : tm->result) {
      BOOST_CHECK_EQUAL(row[0].id, dariadb::Id(0));
      BOOST_CHECK_EQUAL(row[1].id, dariadb::Id(1));
      BOOST_CHECK_EQUAL(row[2].id, dariadb::Id(2));
      BOOST_CHECK_EQUAL(row[3].id, dariadb::Id(3));
      BOOST_CHECK_EQUAL(row[4].id, dariadb::Id(no_exists_id));
	  BOOST_CHECK_EQUAL(row[4].flag, dariadb::FLAGS::_NO_DATA);
      BOOST_CHECK(
          std::all_of(row.begin(), row.end(), [&row](const dariadb::Meas &m) {
            return m.time == row.front().time;
          }));
    }
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}