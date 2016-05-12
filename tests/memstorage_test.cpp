#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include <meas.h>
#include <storage/bloom_filter.h>
#include <storage/memstorage.h>
#include <timeutil.h>
#include <union_storage.h>

#include <flags.h>

#include <atomic>
#include <iostream>
#include <string>
#include <thread>

#include "test_common.h"

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

BOOST_AUTO_TEST_CASE(MemoryStorage) {
  {
    auto ms = new dariadb::storage::MemoryStorage{500};
    const dariadb::Time from = 0; // dariadb::timeutil::current_time();
    const dariadb::Time to = dariadb_test::copies_count * 2; //+ from ;
    const dariadb::Time step = 2;
    dariadb_test::storage_test_check(ms, from, to, step);
    BOOST_CHECK_EQUAL(ms->chunks_size(), (to - from) / step); // id per chunk.
    delete ms;
  }
}

std::atomic_long writed_count{0};
void thread_writer(dariadb::Id id, dariadb::Time from, dariadb::Time to,
                   dariadb::Time step, dariadb::storage::MemoryStorage *ms) {
  auto m = dariadb::Meas::empty();
  for (auto i = from; i < to; i += step) {
    m.id = id;
    m.flag = dariadb::Flag(i);
    m.time = i;
    m.value = dariadb::Value(i);
    ms->append(m);
    writed_count++;
  }
}
bool stop_read_all{false};

void thread_read_all(dariadb::Time from, dariadb::Time to,
                     dariadb::storage::MemoryStorage *ms) {

  while (!stop_read_all) {
    auto rdr = ms->readInterval(from, to);
    dariadb::Meas::MeasList out;
    rdr->readAll(&out);
  }
}

void thread_reader(dariadb::Id id, dariadb::Time from, dariadb::Time to,
                   size_t expected, dariadb::storage::MemoryStorage *ms) {
  dariadb::IdArray ids;
  if (id != 0) {
    ids.push_back(id);
  }

  if (to == 0) {
    auto rdr = ms->readInTimePoint(ids, 0, from);
    dariadb::Meas::MeasList out;
    rdr->readAll(&out);
    BOOST_CHECK_EQUAL(out.size(), expected);
    assert(out.size() == expected);
  } else {
    auto rdr = ms->readInterval(ids, 0, from, to);
    dariadb::Meas::MeasList out;
    rdr->readAll(&out);
    BOOST_CHECK_EQUAL(out.size(), expected);
    assert(out.size() == expected);
  }
}

BOOST_AUTO_TEST_CASE(MultiThread) {
  auto ms = new dariadb::storage::MemoryStorage{500};
  std::thread t1(thread_writer, 0, 0, 100, 2, ms);
  std::thread t2(thread_writer, 1, 0, 100, 2, ms);
  std::thread t3(thread_writer, 2, 0, 100, 2, ms);
  std::thread t4(thread_writer, 0, 100, 200, 1, ms);
  std::thread t5(thread_read_all, 0, 200, ms);

  t1.join();
  t2.join();
  t3.join();
  t4.join();
  stop_read_all = true;
  t5.join();

  std::thread rt1(thread_reader, 0, 0, 200, writed_count.load(), ms);
  std::thread rt2(thread_reader, 1, 50, 0, 1, ms);
  std::thread rt3(thread_reader, 2, 0, 100, 50, ms);
  std::thread rt4(thread_reader, 0, 50, 0, 3, ms);

  rt1.join();
  rt2.join();
  rt3.join();
  rt4.join();

  delete ms;
}

BOOST_AUTO_TEST_CASE(ReadInterval) {
  auto ds = new dariadb::storage::MemoryStorage{500};
  dariadb_test::readIntervalCommonTest(ds);
  delete ds;
}

BOOST_AUTO_TEST_CASE(byStep) {
  const size_t id_count = 1;
  { // equal step
    auto ms = new dariadb::storage::MemoryStorage{500};

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
    auto rdr = ms->readInterval(0, total_count);

    dariadb::Meas::MeasList allByStep;
    rdr = ms->readInterval(0, total_count);
    rdr->readByStep(&allByStep, 0, total_count, time_step);
    auto expected = size_t(total_count / time_step) * id_count; //+ timepoint
    BOOST_CHECK_EQUAL(allByStep.size(), expected);
    delete ms;
  }

  { // less step
    auto ms = new dariadb::storage::MemoryStorage{500};

    auto m = dariadb::Meas::empty();
    const size_t total_count = 100;
    const dariadb::Time time_step = 10;

    for (size_t i = 0; i < total_count; i += time_step) {
      m.id = i % id_count;
      m.flag = dariadb::Flag(i);
      m.time = i;
      m.value = 0;
      ms->append(m);
    }

    auto rdr = ms->readInterval(0, total_count);

    dariadb::Time query_step = 11;
    dariadb::Meas::MeasList allByStep;
    rdr = ms->readInterval(0, total_count);
    rdr->readByStep(&allByStep, 0, total_count, query_step);
    auto expected =
        size_t(total_count / query_step) * id_count + id_count; //+ timepoint;
    BOOST_CHECK_EQUAL(allByStep.size(), expected);
    delete ms;
  }

  { // great step
    auto ms = new dariadb::storage::MemoryStorage{500};

    auto m = dariadb::Meas::empty();
    const size_t total_count = 100;
    const dariadb::Time time_step = 10;

    for (size_t i = 0; i < total_count; i += time_step) {
      m.id = i % id_count;
      m.flag = dariadb::Flag(i);
      m.time = i;
      m.value = 0;
      ms->append(m);
    }

    auto rdr = ms->readInterval(0, total_count);
    dariadb::Meas::MeasList all;
    rdr->readAll(&all);

    dariadb::Time query_step = 5;
    dariadb::Meas::MeasList allByStep;
    rdr = ms->readInterval(0, total_count);
    rdr->readByStep(&allByStep, 0, total_count, query_step);
    auto expected = size_t(total_count / time_step) * 2 * id_count +
                    id_count; //+ timepoint;
    BOOST_CHECK_EQUAL(allByStep.size(), expected);
    delete ms;
  }

  { // from before data
    auto ms = new dariadb::storage::MemoryStorage{500};

    auto m = dariadb::Meas::empty();
    const size_t total_count = 100;
    const dariadb::Time time_step = 10;

    for (size_t i = time_step; i < total_count; i += time_step) {
      m.id = i % id_count;
      m.flag = dariadb::Flag(i);
      m.time = i;
      m.value = 0;
      ms->append(m);
    }

    auto rdr = ms->readInterval(time_step, total_count);
    dariadb::Meas::MeasList all;
    rdr->readAll(&all);

    dariadb::Time query_step = 5;
    dariadb::Meas::MeasList allByStep;
    rdr = ms->readInterval(time_step, total_count);

    rdr->readByStep(&allByStep, 0, total_count, query_step);

    dariadb::Time expected =
        dariadb::Time((total_count - time_step) / time_step) * 2;
    expected = expected * id_count;
    expected += id_count * (time_step / query_step); //+ before first value
    expected += id_count;                            // one after last  value

    BOOST_CHECK_EQUAL(allByStep.size(), expected);
    delete ms;
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

  std::unique_ptr<dariadb::storage::MemoryStorage> ms{
      new dariadb::storage::MemoryStorage{500}};
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

BOOST_AUTO_TEST_CASE(CurValues) {
  {
    auto ms = new dariadb::storage::MemoryStorage{500};

    auto m = dariadb::Meas::empty();
    const size_t total_count = 10;
    const dariadb::Time time_step = 1;

    for (int k = 0; k < 10; k++) {
      for (size_t i = 0; i < total_count; i += time_step) {
        m.id = dariadb::Id(i);
        m.time = dariadb::Time(k);
        m.value = dariadb::Value(i);
        ms->append(m);
      }
    }

    dariadb::IdArray ids;
    auto rdr = ms->currentValue(ids, 0);

    dariadb::Meas::MeasList all;
    rdr->readAll(&all);
    auto expected = total_count;
    BOOST_CHECK_EQUAL(all.size(), expected);
    for (auto v : all) {
      BOOST_CHECK_EQUAL(v.time, dariadb::Time(9));
    }
    delete ms;
  }
}

BOOST_AUTO_TEST_CASE(DropOldChunks) {
  auto ms = new dariadb::storage::MemoryStorage{500};
  auto m = dariadb::Meas::empty();
  auto t = dariadb::timeutil::current_time();
  for (auto i = 0; i < 1000; i += 1, t += 100) {
    m.id = i;
    m.flag = dariadb::Flag(i);
    m.src = dariadb::Flag(i);
    m.time = t;
    m.value = 0;
    for (size_t j = 1; j < 1000; j++) {
      m.value = dariadb::Value(j);
      m.time++;
      ms->append(m);
    }
  }
  const dariadb::Time min_time = 10;
  auto before_size = ms->chunks_total_size();
  auto before_min = ms->minTime();

  auto chunks = ms->drop_old_chunks(min_time);
  auto now = dariadb::timeutil::current_time();
  BOOST_CHECK(chunks.size() > 0);
  for (auto c : chunks) {
    auto flg = c->info.maxTime <= (now - min_time);
    BOOST_CHECK(flg);
  }
  auto after_size = ms->chunks_total_size();
  BOOST_CHECK(before_size > after_size);
  BOOST_CHECK(ms->minTime() > before_min);

  delete ms;
}

BOOST_AUTO_TEST_CASE(DropByLimitChunks) {
  auto ms = new dariadb::storage::MemoryStorage{500};
  auto m = dariadb::Meas::empty();
  auto t = 0;
  size_t max_limit = 100;
  for (auto i = 0; i < 1000; i += 1, t += 10) {
    m.id = i;
    m.flag = dariadb::Flag(i);
    m.src = dariadb::Flag(i);
    m.time = t;
    m.value = 0;
    for (size_t j = 1; j < 1000; j++) {
      m.value = dariadb::Value(j);
      m.time++;
      ms->append(m);
    }
    if (ms->chunks_total_size() > max_limit * 2) {
      break;
    }
  }
  auto droped = ms->drop_old_chunks_by_limit(max_limit);
  BOOST_CHECK(ms->chunks_total_size() <= max_limit);
  dariadb::Meas::MeasList out;
  ms->readInterval(t, m.time)->readAll(&out);
  BOOST_CHECK(out.size() > size_t(0));
  delete ms;
}
