#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include "test_common.h"
#include <boost/test/unit_test.hpp>

#include <engine.h>
#include <storage/bloom_filter.h>
#include <timeutil.h>
#include <utils/fs.h>
#include <utils/logger.h>

class BenchCallback : public dariadb::storage::ReaderClb {
public:
  BenchCallback() { count = 0; }
  void call(const dariadb::Meas &) { count++; }
  size_t count;
};

BOOST_AUTO_TEST_CASE(BloomTest) {
  typedef uint8_t u8_fltr_t;

  auto u8_fltr = dariadb::storage::bloom_empty<u8_fltr_t>();

  BOOST_CHECK_EQUAL(u8_fltr, uint8_t{0});

  u8_fltr = dariadb::storage::bloom_add(u8_fltr, uint8_t{1});
  u8_fltr = dariadb::storage::bloom_add(u8_fltr, uint8_t{2});

  BOOST_CHECK(dariadb::storage::bloom_check(u8_fltr, uint8_t{1}));
  BOOST_CHECK(dariadb::storage::bloom_check(u8_fltr, uint8_t{2}));
  BOOST_CHECK(dariadb::storage::bloom_check(u8_fltr, uint8_t{3}));
  BOOST_CHECK(!dariadb::storage::bloom_check(u8_fltr, uint8_t{4}));
  BOOST_CHECK(!dariadb::storage::bloom_check(u8_fltr, uint8_t{5}));
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

BOOST_AUTO_TEST_CASE(Engine_common_test) {
  const std::string storage_path = "testStorage";
  const size_t chunk_per_storage = 10000;
  const size_t chunk_size = 256;
  const size_t cap_B = 5;

  const dariadb::Time from = 0;
  const dariadb::Time to = from + 1021;
  const dariadb::Time step = 10;

  {
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    dariadb::storage::MeasStorage_ptr ms{new dariadb::storage::Engine(
        dariadb::storage::PageManager::Params(storage_path, chunk_per_storage,
                                              chunk_size),
        dariadb::storage::Capacitor::Params(cap_B, storage_path),
        dariadb::storage::Engine::Limits(10))};

    dariadb_test::storage_test_check(ms.get(), from, to, step);
  }
  {
    dariadb::storage::MeasStorage_ptr ms{new dariadb::storage::Engine(
        dariadb::storage::PageManager::Params(storage_path, chunk_per_storage,
                                              chunk_size),
        dariadb::storage::Capacitor::Params(cap_B, storage_path),
        dariadb::storage::Engine::Limits(0))};

    dariadb::Meas::MeasList mlist;
    ms->currentValue(dariadb::IdArray{}, 0)->readAll(&mlist);
    BOOST_CHECK(mlist.size() == size_t(1));
    BOOST_CHECK(mlist.front().flag != dariadb::Flags::_NO_DATA);
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

class Moc_SubscribeClbk : public dariadb::storage::ReaderClb {
public:
  std::list<dariadb::Meas> values;
  void call(const dariadb::Meas &m) override { values.push_back(m); }
  ~Moc_SubscribeClbk() {}
};

BOOST_AUTO_TEST_CASE(Subscribe) {
  const size_t id_count = 5;
  std::shared_ptr<Moc_SubscribeClbk> c1(new Moc_SubscribeClbk);
  std::shared_ptr<Moc_SubscribeClbk> c2(new Moc_SubscribeClbk);
  std::shared_ptr<Moc_SubscribeClbk> c3(new Moc_SubscribeClbk);
  std::shared_ptr<Moc_SubscribeClbk> c4(new Moc_SubscribeClbk);

  const std::string storage_path = "testStorage";
  const size_t chunk_per_storage = 10000;
  const size_t chunk_size = 256;
  const size_t cap_B = 5;

  {
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    dariadb::storage::MeasStorage_ptr ms{new dariadb::storage::Engine(
        dariadb::storage::PageManager::Params(storage_path, chunk_per_storage,
                                              chunk_size),
        dariadb::storage::Capacitor::Params(cap_B, storage_path),
        dariadb::storage::Engine::Limits(10))};

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