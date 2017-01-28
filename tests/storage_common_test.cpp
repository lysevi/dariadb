#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include "test_common.h"
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <iostream>
#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/bystep/step_kind.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/utils/fs.h>

BOOST_AUTO_TEST_CASE(BloomTest) {
  uint64_t u8_fltr = dariadb::storage::bloom_empty<uint8_t>();

  BOOST_CHECK_EQUAL(u8_fltr, uint64_t{0});

  u8_fltr = dariadb::storage::bloom_add(u8_fltr, uint8_t{1});
  u8_fltr = dariadb::storage::bloom_add(u8_fltr, uint8_t{2});
  u8_fltr = dariadb::storage::bloom_add(u8_fltr, uint8_t{3});

  BOOST_CHECK(dariadb::storage::bloom_check(u8_fltr, uint8_t{1}));
  BOOST_CHECK(dariadb::storage::bloom_check(u8_fltr, uint8_t{2}));
  BOOST_CHECK(dariadb::storage::bloom_check(u8_fltr, uint8_t{3}));

  uint64_t u8_fltr_2 = dariadb::storage::bloom_empty<uint8_t>();
  u8_fltr_2 = dariadb::storage::bloom_add(u8_fltr_2, uint8_t{4});

  BOOST_CHECK(!dariadb::storage::bloom_check(u8_fltr, uint8_t{4}));
  BOOST_CHECK(dariadb::storage::bloom_check(u8_fltr_2, uint8_t{4}));

  auto super_fltr = dariadb::storage::bloom_combine(u8_fltr, u8_fltr_2);
  BOOST_CHECK(dariadb::storage::bloom_check(super_fltr, uint8_t{1}));
  BOOST_CHECK(dariadb::storage::bloom_check(super_fltr, uint8_t{2}));
  BOOST_CHECK(dariadb::storage::bloom_check(super_fltr, uint8_t{3}));
  BOOST_CHECK(dariadb::storage::bloom_check(super_fltr, uint8_t{4}));
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

BOOST_AUTO_TEST_CASE(StatisticUpdate) {
  dariadb::storage::Statistic st;

  BOOST_CHECK_EQUAL(st.minTime, dariadb::MAX_TIME);
  BOOST_CHECK_EQUAL(st.maxTime, dariadb::MIN_TIME);
  BOOST_CHECK_EQUAL(st.count, uint32_t(0));
  BOOST_CHECK_EQUAL(st.flag_bloom, dariadb::Flag(0));
  BOOST_CHECK_EQUAL(st.minValue, dariadb::MAX_VALUE);
  BOOST_CHECK_EQUAL(st.maxValue, dariadb::MIN_VALUE);
  BOOST_CHECK_EQUAL(st.sum, dariadb::Value(0));

  auto m = dariadb::Meas::empty(0);
  m.time = 2;
  m.flag = 2;
  m.value = 2;
  st.update(m);
  BOOST_CHECK_EQUAL(st.minTime, m.time);
  BOOST_CHECK_EQUAL(st.maxTime, m.time);
  BOOST_CHECK(st.flag_bloom != dariadb::Flag(0));
  BOOST_CHECK(dariadb::areSame(st.minValue, m.value));
  BOOST_CHECK(dariadb::areSame(st.maxValue, m.value));

  m.time = 3;
  m.value = 3;
  st.update(m);
  BOOST_CHECK_EQUAL(st.minTime, dariadb::Time(2));
  BOOST_CHECK_EQUAL(st.maxTime, dariadb::Time(3));
  BOOST_CHECK(dariadb::areSame(st.minValue, dariadb::Value(2)));
  BOOST_CHECK(dariadb::areSame(st.maxValue, dariadb::Value(3)));

  m.time = 1;
  m.value = 1;
  st.update(m);
  BOOST_CHECK_EQUAL(st.minTime, dariadb::Time(1));
  BOOST_CHECK_EQUAL(st.maxTime, dariadb::Time(3));
  BOOST_CHECK(dariadb::areSame(st.minValue, dariadb::Value(1)));
  BOOST_CHECK(dariadb::areSame(st.maxValue, dariadb::Value(3)));
  BOOST_CHECK(dariadb::areSame(st.sum, dariadb::Value(6)));
  BOOST_CHECK_EQUAL(st.count, uint32_t(3));

  dariadb::storage::Statistic second_st;
  m.time = 777;
  m.value = 1;
  second_st.update(m);
  BOOST_CHECK_EQUAL(second_st.maxTime, m.time);

  second_st.update(st);
  BOOST_CHECK_EQUAL(second_st.minTime, dariadb::Time(1));
  BOOST_CHECK_EQUAL(second_st.maxTime, dariadb::Time(777));
  BOOST_CHECK(dariadb::areSame(second_st.minValue, dariadb::Value(1)));
  BOOST_CHECK(dariadb::areSame(second_st.maxValue, dariadb::Value(3)));
  BOOST_CHECK(dariadb::areSame(second_st.sum, dariadb::Value(7)));
  BOOST_CHECK_EQUAL(second_st.count, uint32_t(4));
}

BOOST_AUTO_TEST_CASE(ManifestFileTest) {
  const std::string storage_path = "emptyStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  std::string version = "0.1.2.3.4.5";

  {
    auto settings = dariadb::storage::Settings::create(storage_path);
    auto manifest = dariadb::storage::Manifest::create(settings);
    std::list<std::string> pages_names{"1", "2", "3"};
    for (auto n : pages_names) {
      manifest->page_append(n);
    }

    manifest->set_format(version);

    std::list<std::string> wal_names{"111", "222", "333"};
    for (auto n : wal_names) {
      manifest->wal_append(n);
    }

    auto page_lst = manifest->page_list();
    BOOST_CHECK_EQUAL(page_lst.size(), pages_names.size());
    BOOST_CHECK_EQUAL_COLLECTIONS(page_lst.begin(), page_lst.end(),
                                  pages_names.begin(), pages_names.end());

    auto wal_lst = manifest->wal_list();
    BOOST_CHECK_EQUAL(wal_lst.size(), wal_names.size());
    BOOST_CHECK_EQUAL_COLLECTIONS(wal_lst.begin(), wal_lst.end(),
                                  wal_names.begin(), wal_names.end());

    manifest = nullptr;
  }
  { // reopen. restore method must remove all records from manifest.
    auto settings = dariadb::storage::Settings::create(storage_path);
    auto manifest = dariadb::storage::Manifest::create(settings);
    BOOST_CHECK_EQUAL(manifest->page_list().size(), size_t(0));
    BOOST_CHECK_EQUAL(manifest->wal_list().size(), size_t(0));
    BOOST_CHECK_EQUAL(manifest->get_format(), version);
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

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

  bool file_exists =
      dariadb::utils::fs::path_exists(dariadb::utils::fs::append_path(
          storage_path, dariadb::storage::SETTINGS_FILE_NAME));
  BOOST_CHECK(file_exists);

  settings = dariadb::storage::Settings::create(storage_path);
  BOOST_CHECK_EQUAL(settings->wal_cache_size.value(), uint64_t(2));
  BOOST_CHECK_EQUAL(settings->chunk_size.value(), uint32_t(7));
  BOOST_CHECK(settings->strategy.value() ==
              dariadb::storage::STRATEGY::COMPRESSED);

  settings = nullptr;
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}
