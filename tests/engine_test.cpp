#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include "test_common.h"
#include <boost/test/unit_test.hpp>

#include <libdariadb/engine.h>
#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/bystep/step_kind.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/storage/pages/page_manager.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>
#include <algorithm>
#include <iostream>

class BenchCallback : public dariadb::storage::IReaderClb {
public:
  BenchCallback() { count = 0; }
  void call(const dariadb::Meas &) { count++; }
  size_t count;
};

BOOST_AUTO_TEST_CASE(ManifestStoreSteps) {
  const std::string storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  {
    dariadb::utils::fs::mkdir(storage_path);
    auto s = dariadb::storage::Settings::create(storage_path);
    auto m = dariadb::storage::Manifest::create(s);

    dariadb::storage::Id2Step id2step;
    dariadb::Id id_val = 0;
    for (auto i = 0; i < 100; ++i) {
      auto bs_id = id_val + 100000;
      id2step[bs_id] = dariadb::storage::STEP_KIND::MINUTE;
      ++id_val;
    }
    m->insert_id2step(id2step);
    id2step = m->read_id2step();
    BOOST_CHECK_EQUAL(id2step.size(), size_t(100));
    BOOST_CHECK_EQUAL(id2step[100000], dariadb::storage::STEP_KIND::MINUTE);
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(BloomTest) {
  size_t u8_fltr = dariadb::storage::bloom_empty<uint8_t>();

  BOOST_CHECK_EQUAL(u8_fltr, size_t{0});

  u8_fltr = dariadb::storage::bloom_add(u8_fltr, uint8_t{1});
  u8_fltr = dariadb::storage::bloom_add(u8_fltr, uint8_t{2});
  u8_fltr = dariadb::storage::bloom_add(u8_fltr, uint8_t{3});

  BOOST_CHECK(dariadb::storage::bloom_check(u8_fltr, uint8_t{1}));
  BOOST_CHECK(dariadb::storage::bloom_check(u8_fltr, uint8_t{2}));
  BOOST_CHECK(dariadb::storage::bloom_check(u8_fltr, uint8_t{3}));
}

BOOST_AUTO_TEST_CASE(inFilter) {
  {
    auto m = dariadb::Meas::empty();
    m.flag = 100;
    BOOST_CHECK(m.inFlag(0));
    BOOST_CHECK(m.inFlag(100));
    BOOST_CHECK(!m.inFlag(10));
  }
}

BOOST_AUTO_TEST_CASE(Options_Instance) {

  const std::string storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  dariadb::utils::fs::mkdir(storage_path);

  auto settings = dariadb::storage::Settings::create(storage_path);

  settings->wal_cache_size.setValue(2);
  settings->chunk_size.setValue(7);
  settings->strategy.setValue(dariadb::storage::STRATEGY::COMPRESSED);
  settings->save();

  settings = nullptr;

  bool file_exists = dariadb::utils::fs::path_exists(dariadb::utils::fs::append_path(
      storage_path, dariadb::storage::SETTINGS_FILE_NAME));
  BOOST_CHECK(file_exists);

  settings = dariadb::storage::Settings::create(storage_path);
  BOOST_CHECK_EQUAL(settings->wal_cache_size.value(), uint64_t(2));
  BOOST_CHECK_EQUAL(settings->chunk_size.value(), uint32_t(7));
  BOOST_CHECK(settings->strategy.value() == dariadb::storage::STRATEGY::COMPRESSED);

  settings = nullptr;
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(Engine_common_test) {
  const std::string storage_path = "testStorage";
  const size_t chunk_size = 256;

  const dariadb::Time from = 0;
  const dariadb::Time to = from + 1000;
  const dariadb::Time step = 10;

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

    dariadb_test::storage_test_check(ms.get(), from, to, step, true);

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

    dariadb::storage::IMeasStorage_ptr ms{raw_ptr};
    auto index_files = dariadb::utils::fs::ls(storage_path, ".pagei");
    for (auto &f : index_files) {
      dariadb::utils::fs::rm(f);
    }
    raw_ptr->fsck();

    // check first id, because that Id placed in compressed pages.
    auto values = ms->readInterval(QueryInterval({dariadb::Id(0)}, 0, from, to));
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
    settings->strategy.setValue(dariadb::storage::STRATEGY::WAL);
    std::unique_ptr<Engine> ms{new Engine(settings)};

    dariadb::IdSet all_ids;
    dariadb::Time maxWritedTime;
    dariadb_test::fill_storage_for_test(ms.get(), from, to, step, &all_ids,
                                        &maxWritedTime);

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

class Moc_SubscribeClbk : public dariadb::storage::IReaderClb {
public:
  std::list<dariadb::Meas> values;
  void call(const dariadb::Meas &m) override { values.push_back(m); }
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

    auto ms = std::make_shared<dariadb::storage::Engine>(settings);

    dariadb::IdArray ids{};
    ms->subscribe(ids, 0, c1); // all
    ids.push_back(2);
    ms->subscribe(ids, 0, c2); // 2
    ids.push_back(1);
    ms->subscribe(ids, 0, c3); // 1 2
    ids.clear();
    ms->subscribe(ids, 1, c4); // with flag=1

    auto m = dariadb::Meas::empty();
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

    dariadb_test::storage_test_check(ms.get(), from, to, step, true);

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

    dariadb_test::storage_test_check(ms.get(), from, to, step, true);

    auto descr = ms->description();
    BOOST_CHECK_GT(descr.pages_count, size_t(0));
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(Engine_ByStep_common_test) {
  const std::string storage_path = "testStorage";

  const size_t chunk_size = 128;
  const dariadb::Time from = 0;
  const dariadb::Time to = from + 1000;
  const dariadb::Time step = 10;

  using namespace dariadb::storage;

  const dariadb::Id spec_id = 777;
  {
    std::cout << "Engine_ByStep_common_test\n";
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    auto settings = dariadb::storage::Settings::create(storage_path);
    settings->strategy.setValue(STRATEGY::MEMORY);
    settings->chunk_size.setValue(chunk_size);
    settings->memory_limit.setValue(50 * 1024);
    settings->wal_file_size.setValue(2000);
    std::unique_ptr<Engine> ms{new Engine(settings)};

    dariadb::storage::Id2Step id2step;
    dariadb::Id id_val = 0;
    for (auto i = from; i < to; i += step) {
      auto bs_id = id_val + 100000;
      id2step[bs_id] = dariadb::storage::STEP_KIND::SECOND;
      ++id_val;
    }

    ms->setSteps(id2step);
    // must add spec_id steps to exists map;
    id2step[spec_id] = dariadb::storage::STEP_KIND::SECOND;
    dariadb_test::storage_test_check(ms.get(), from, to, step, true);

    auto descr = ms->description();
    BOOST_CHECK_GT(descr.pages_count, size_t(0));
  }
  {
    std::cout << "Reopen storage to load id2step\n";
    auto settings = dariadb::storage::Settings::create(storage_path);
    std::unique_ptr<Engine> ms{new Engine(settings)};

    QueryInterval qi({}, 0, from, to);
    qi.ids.resize(1);
    qi.ids[0] = dariadb::Id(100000);
    auto mlist = ms->readInterval(qi);
    BOOST_CHECK(mlist.empty());

    auto v = dariadb::Meas::empty(qi.ids[0]);
    v.time = 0;
    v.value = 777;
    ms->append(v);
    mlist = ms->readInterval(qi);
    BOOST_CHECK(!mlist.empty());
    BOOST_CHECK_EQUAL(mlist.front().value, v.value);

    v.id = spec_id;
    v.time = 0;
    v.value = 777;
    ms->append(v);

    qi.ids[0] = dariadb::Id(spec_id);
    mlist = ms->readInterval(qi);
    BOOST_CHECK(!mlist.empty());
    BOOST_CHECK_EQUAL(mlist.front().value, v.value);
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}
