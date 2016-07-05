#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include "test_common.h"
#include <boost/test/unit_test.hpp>

#include <engine.h>
#include <storage/bloom_filter.h>
#include <storage/page_manager.h>
#include <timeutil.h>
#include <utils/fs.h>
#include <utils/logger.h>
#include <storage/lock_manager.h>
#include <algorithm>

class BenchCallback : public dariadb::storage::ReaderClb {
public:
  BenchCallback() { count = 0; }
  void call(const dariadb::Meas &) { count++; }
  size_t count;
};

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

BOOST_AUTO_TEST_CASE(QueryHashTest) {
  dariadb::IdArray ids1{dariadb::Id(0), dariadb::Id(1), dariadb::Id(2), dariadb::Id(3)};
  {
    dariadb::storage::IdArrayHasher hasher;
    auto h1 = hasher(ids1);
    BOOST_CHECK(h1 != 0);

    auto h1_2 = hasher(ids1);
    BOOST_CHECK_EQUAL(h1, h1_2);

    std::swap(ids1[0], ids1[3]);
    auto h2 = hasher(ids1);
    BOOST_CHECK(h2 != 0);
    BOOST_CHECK(h1 != h2);
  }
  {
    dariadb::storage::QueryInterval qi1(ids1, 1, 0, 10);
    dariadb::storage::QueryInterval qi2(ids1, 1, 0, 10);

    dariadb::storage::QueryIntervalHasher hasher;
    BOOST_CHECK_EQUAL(hasher(qi1), hasher(qi2));

    qi2.flag++;
    BOOST_CHECK(hasher(qi1) != hasher(qi2));
    qi2.flag--;
    BOOST_CHECK_EQUAL(hasher(qi1), hasher(qi2));
    qi2.from++;
    BOOST_CHECK(hasher(qi1) != hasher(qi2));
    qi2.from--;
    BOOST_CHECK_EQUAL(hasher(qi1), hasher(qi2));
    qi2.to++;
    BOOST_CHECK(hasher(qi1) != hasher(qi2));
  }

  {
    dariadb::storage::QueryTimePoint q1(ids1, 1, 10);
    dariadb::storage::QueryTimePoint q2(ids1, 1, 10);

    dariadb::storage::QueryTimePointHasher hasher;
    BOOST_CHECK_EQUAL(hasher(q1), hasher(q2));

    q2.flag++;
    BOOST_CHECK(hasher(q1) != hasher(q2));
    q2.flag--;
    BOOST_CHECK_EQUAL(hasher(q1), hasher(q2));
    q2.time_point++;
    BOOST_CHECK(hasher(q1) != hasher(q2));
    q2.time_point--;
    BOOST_CHECK_EQUAL(hasher(q1), hasher(q2));
  }
}

BOOST_AUTO_TEST_CASE(LockManager_Instance) {
  BOOST_CHECK(dariadb::storage::LockManager::instance() == nullptr);
  dariadb::storage::LockManager::start(dariadb::storage::LockManager::Params());
  BOOST_CHECK(dariadb::storage::LockManager::instance() != nullptr);
  dariadb::storage::LockManager::stop();
  BOOST_CHECK(dariadb::storage::LockManager::instance() == nullptr);
}

BOOST_AUTO_TEST_CASE(Engine_common_test) {
  const std::string storage_path = "testStorage";
  const size_t chunk_per_storage = 10000;
  const size_t chunk_size = 256;
  const size_t cap_B = 2;

  const dariadb::Time from = 0;
  const dariadb::Time to = from + 1500;
  const dariadb::Time step = 10;

  {
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    dariadb::storage::CapacitorManager::Params cap_pam(storage_path, cap_B);
    cap_pam.max_levels = 4;
    dariadb::storage::AOFManager::Params aofp(storage_path, chunk_size);
    aofp.max_closed_aofs = 20;
    aofp.max_size=cap_pam.measurements_count();
    dariadb::storage::MeasStorage_ptr ms{new dariadb::storage::Engine(
        aofp, dariadb::storage::PageManager::Params(storage_path, chunk_per_storage,
                                                    chunk_size),
        cap_pam, dariadb::storage::Engine::Limits(10))};

    dariadb_test::storage_test_check(ms.get(), from, to, step);

    auto pages_count = dariadb::storage::PageManager::instance()->files_count();
    BOOST_CHECK_GE(pages_count, size_t(1));
  }
  {
    dariadb::storage::MeasStorage_ptr ms{new dariadb::storage::Engine(
        dariadb::storage::AOFManager::Params(storage_path, chunk_size),
        dariadb::storage::PageManager::Params(storage_path, chunk_per_storage,
                                              chunk_size),
        dariadb::storage::CapacitorManager::Params(storage_path, cap_B),
        dariadb::storage::Engine::Limits(0))};

    dariadb::Meas::MeasList mlist;
    ms->currentValue(dariadb::IdArray{}, 0)->readAll(&mlist);
    BOOST_CHECK(mlist.size() != size_t(0));
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
        dariadb::storage::AOFManager::Params(storage_path, chunk_size),
        dariadb::storage::PageManager::Params(storage_path, chunk_per_storage,
                                              chunk_size),
        dariadb::storage::CapacitorManager::Params(storage_path, cap_B),
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

BOOST_AUTO_TEST_CASE(Engine_unordered_test) {
  const std::string storage_path = "testStorage";
  const size_t chunk_per_storage = 10000;
  const size_t chunk_size = 256;
  const size_t cap_B = 5;

  {
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    dariadb::storage::AOFManager::Params aofp(storage_path, chunk_size);
    dariadb::storage::PageManager::Params pmp(storage_path, chunk_per_storage,
                                                  chunk_size);
    dariadb::storage::CapacitorManager::Params cap_pam(storage_path, cap_B);
    aofp.max_size=cap_pam.measurements_count();
    dariadb::storage::MeasStorage_ptr ms{new dariadb::storage::Engine(
                    aofp,
        pmp,cap_pam,
        dariadb::storage::Engine::Limits(10))};

    // storage: id=0: 10,11,....30 id=1: 0,1,2,3,4,5
    // cap: id=0: 6,7,8,9

    auto m = dariadb::Meas::empty(0);
    dariadb::Time t = 10;
    // all must be in first chunk.
    for (; t <= 30; ++t) {
      m.time = t;
      m.value = dariadb::Value(t);
      BOOST_CHECK(ms->append(m).writed == 1);
    }
    m.id++;
    t = 0;
    // fill capacitor, while she dont drop values to page
    while (dariadb::storage::PageManager::instance()->chunks_in_cur_page() < 2) {
      m.time = t;
      m.value = dariadb::Value(t);
      t++;
      BOOST_CHECK(ms->append(m).writed == 1);
    }
    auto last_chunks_count =
        dariadb::storage::PageManager::instance()->chunks_in_cur_page();
    m.id = 0;
    // must be in second chunk in page
    auto new_t = dariadb::Time(0);
    for (; new_t <= 5; ++new_t) {
      m.time = new_t;
      m.value = dariadb::Value(new_t);
      BOOST_CHECK(ms->append(m).writed == 1);
    }
    m.id++;
    ++t;
    while (dariadb::storage::PageManager::instance()->chunks_in_cur_page() <=
           size_t(last_chunks_count * 1.5)) {
      m.time = t;
      m.value = dariadb::Value(t);
      t++;
      BOOST_CHECK(ms->append(m).writed == 1);
    }
    auto new_chunks_count =
        dariadb::storage::PageManager::instance()->chunks_in_cur_page();
    BOOST_CHECK(last_chunks_count < new_chunks_count);

    new_t = dariadb::Time(6);
    m.id = 0;
    for (; new_t <= 9; ++new_t) {
      m.time = new_t;
      m.value = dariadb::Value(new_t);
      BOOST_CHECK(ms->append(m).writed == 1);
    }
    dariadb::IdArray ids;
    ids.push_back(0);
    auto reader = ms->readInterval(dariadb::storage::QueryInterval(ids, 0, 0, 11));
    dariadb::Meas::MeasList mlist;
    reader->readAll(&mlist);

    for (auto it = mlist.begin(); it != mlist.end(); ++it) {
      auto next = ++it;
      if (next == mlist.end()) {
        break;
      }
      BOOST_CHECK_LE(it->time, next->time);
    }
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(Engine_common_test_rnd) {
  const std::string storage_path = "testStorage";
  const size_t chunk_per_storage = 10000;
  const size_t chunk_size = 256;
  const size_t cap_B = 5;

  const dariadb::Time from = 0;
  const dariadb::Time to = 1000;
  const dariadb::Time step = 10;
  const size_t copies_for_id = 100;
  {
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    dariadb::storage::CapacitorManager::Params cap_pam(storage_path, cap_B);
    cap_pam.max_levels = 4;
    dariadb::storage::AOFManager::Params aofp(storage_path, chunk_size);
    aofp.max_closed_aofs = 20;
    aofp.max_size=cap_pam.measurements_count();
    dariadb::storage::MeasStorage_ptr ms{new dariadb::storage::Engine(
        aofp, dariadb::storage::PageManager::Params(storage_path, chunk_per_storage,
                                                    chunk_size),
        cap_pam, dariadb::storage::Engine::Limits(10))};
    auto m = dariadb::Meas::empty();
    size_t total_count = 0;

    dariadb::Id id_val(0);

    dariadb::Flag flg_val(0);
    dariadb::IdSet _all_ids_set;
    std::vector<dariadb::Time> rnd_times;
    rnd_times.resize(copies_for_id + 2);
    dariadb::Time t = 0;
    for (size_t i = 0; i < rnd_times.size(); ++i) {
      rnd_times[i] = t;
      ++t;
    }
    size_t t_pos = 0;
    for (auto i = from; i < to; i += step) {
      std::random_shuffle(rnd_times.begin(), rnd_times.end());
      t_pos = 0;
      _all_ids_set.insert(id_val);
      m.id = id_val;
      m.flag = flg_val;
      m.src = flg_val;
      m.time = rnd_times[t_pos++];
      m.value = 0;

      for (size_t j = 0; j < copies_for_id; j++) {
        BOOST_CHECK(ms->append(m).writed == 1);
        total_count++;
        m.value = dariadb::Value(j);
        m.time = rnd_times[t_pos++];
      }
      ++id_val;
      ++flg_val;
    }
	ms->flush();
    size_t total_readed = 0;
    for (dariadb::Id cur_id = 0; cur_id < id_val; ++cur_id) {
      dariadb::IdArray ids;
      ids.push_back(cur_id);
      auto reader = ms->readInterval(dariadb::storage::QueryInterval(ids, 0, from, to));
      dariadb::Meas::MeasList mlist;
      reader->readAll(&mlist);
      total_readed += mlist.size();
      if (mlist.size() != copies_for_id) {
        BOOST_CHECK(mlist.size() == copies_for_id);
      }
      for (auto it = mlist.begin(); it != mlist.end(); ++it) {
        auto next = ++it;
        if (next == mlist.end()) {
          break;
        }
        BOOST_CHECK_LE(it->time, next->time);
      }
    }
    BOOST_CHECK_GE(total_count, total_readed);
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(Engine_memvalues) {
  const std::string storage_path = "testStorage";
  const size_t chunk_per_storage = 10000;
  const size_t chunk_size = 256;
  const size_t cap_B = 5;

  const dariadb::Time from = 0;
  const dariadb::Time to = 1000;
  const dariadb::Time step = 10;
  {
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    dariadb::storage::AOFManager::Params aofp(storage_path, chunk_size);
    dariadb::storage::PageManager::Params pamp(storage_path, chunk_per_storage,
                                          chunk_size);
    dariadb::storage::CapacitorManager::Params capm(storage_path, cap_B);
    aofp.max_size=capm.measurements_count();
    std::unique_ptr<dariadb::storage::Engine> ms{new dariadb::storage::Engine(
       aofp,pamp,capm,
        dariadb::storage::Engine::Limits(10))};

    auto m = dariadb::Meas::empty();

    dariadb::Id id_val(0);

    dariadb::Flag flg_val(0);
    dariadb::IdSet _all_ids_set;
    for (auto i = from; i < to; i += step) {
      _all_ids_set.insert(id_val);
      m.id = id_val;
      m.flag = flg_val;
      m.src = flg_val;
      m.time = i;
      m.value = 0;
      BOOST_CHECK_EQUAL(ms->append(m).writed, size_t(1));
      ++id_val;
      ++flg_val;
    }
	ms->flush();
    dariadb::IdArray ids{_all_ids_set.begin(), _all_ids_set.end()};
    {
      dariadb::storage::QueryInterval qi(ids, 0, from, to);
      auto query_id1 = ms->load(qi);
      auto query_id2 = ms->load(qi);

      BOOST_CHECK(query_id1 != query_id2);

      dariadb::Meas::MeasList all1, all2;
      all1 = ms->getResult(query_id1);
      all2 = ms->getResult(query_id2);
      BOOST_CHECK_EQUAL(all1.size(), all2.size());
      BOOST_CHECK(all1.size() != size_t(0));
    }
    {
      dariadb::storage::QueryTimePoint qt(ids, 0, from + (to - from) / 2);
      auto query_id1 = ms->load(qt);
      auto query_id2 = ms->load(qt);

      BOOST_CHECK(query_id1 != query_id2);

      dariadb::Meas::MeasList all1, all2;
      all1 = ms->getResult(query_id1);
      all2 = ms->getResult(query_id2);
      BOOST_CHECK_EQUAL(all1.size(), all2.size());
      BOOST_CHECK(all1.size() != size_t(0));
    }
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}
