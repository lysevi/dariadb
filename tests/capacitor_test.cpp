#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <atomic>
#include <boost/test/unit_test.hpp>
#include <cassert>
#include <map>
#include <thread>

#include "test_common.h"
#include <math/statistic.h>
#include <storage/capacitor.h>
#include <timeutil.h>
#include <utils/fs.h>
#include <utils/logger.h>

class Moc_Storage : public dariadb::storage::MeasWriter {
public:
  size_t writed_count;
  std::map<dariadb::Id, std::vector<dariadb::Meas>> meases;
  std::list<dariadb::Meas> mlist;
  dariadb::append_result append(const dariadb::Meas &value) override {
    meases[value.id].push_back(value);
    mlist.push_back(value);
    writed_count += 1;
    return dariadb::append_result(1, 0);
  }

  void flush() override {}
};

BOOST_AUTO_TEST_CASE(CapacitorInitTest) {
  std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
  stor->writed_count = 0;
  const size_t block_size = 10;
  auto storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  auto cap_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::CAP_FILE_EXT);
  assert(cap_files.size() == 0);
  auto p = dariadb::storage::Capacitor::Params(block_size, storage_path);
  p.max_levels = 11;
  size_t writes_count = 10000;

  dariadb::IdSet id_set;
  {

    dariadb::storage::Capacitor cap(stor.get(), p);

    cap_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::CAP_FILE_EXT);
    BOOST_CHECK_EQUAL(cap_files.size(), size_t(1));

    BOOST_CHECK_EQUAL(cap.levels_count(), size_t(p.max_levels));

    auto e = dariadb::Meas::empty();

    dariadb::Time t = writes_count;
    size_t id_count = 10;

    for (size_t i = 0; i < writes_count; i++) {
      e.id = i % id_count;
      id_set.insert(e.id);
      e.time = t;
      e.value = dariadb::Value(i);
      t -= 1;
      BOOST_CHECK(cap.append(e).writed == 1);
    }
    BOOST_CHECK_EQUAL(cap.size(), writes_count);

    dariadb::Meas::MeasList out;

    auto reader = cap.readInterval(dariadb::storage::QueryInterval(
        dariadb::IdArray(id_set.begin(), id_set.end()), 0, 0, writes_count));
    BOOST_CHECK(reader != nullptr);
    reader->readAll(&out);
    BOOST_CHECK_EQUAL(out.size(), cap.size());
  }
  {
    p.max_levels = 12;
    dariadb::storage::Capacitor cap(stor.get(), p);

    cap_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::CAP_FILE_EXT);
    BOOST_CHECK_EQUAL(cap_files.size(), size_t(1));
    // level count must be reade from file header.
    BOOST_CHECK(cap.levels_count() != size_t(p.max_levels));

    BOOST_CHECK_EQUAL(cap.size(), writes_count);
    auto e = dariadb::Meas::empty();

    e.time = writes_count - 1;
    BOOST_CHECK(cap.append(e).writed == 1);

    dariadb::Meas::MeasList out;
    auto ids = dariadb::IdArray(id_set.begin(), id_set.end());
    dariadb::storage::QueryInterval qi{ids, 0, 0, writes_count};
    auto reader = cap.readInterval(qi);
    BOOST_CHECK(reader != nullptr);
    reader->readAll(&out);
    BOOST_CHECK_EQUAL(out.size(), cap.size());
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(CapacitorCommonTest) {
  std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
  stor->writed_count = 0;
  const size_t block_size = 10;
  auto storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  {
    auto cap_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::CAP_FILE_EXT);
    assert(cap_files.size() == 0);
    auto p = dariadb::storage::Capacitor::Params(block_size, storage_path);
    p.max_levels = 11;

    dariadb::storage::Capacitor cap(stor.get(), p);

    dariadb_test::storage_test_check(&cap, 0, 100, 1);
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(CapacitorDropMeasTest) {
  std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
  stor->writed_count = 0;
  const size_t block_size = 10;
  auto storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  {
    auto cap_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::CAP_FILE_EXT);
    assert(cap_files.size() == 0);
    auto p = dariadb::storage::Capacitor::Params(block_size, storage_path);
    p.max_levels = 11;

    dariadb::storage::Capacitor cap(stor.get(), p);

    auto e = dariadb::Meas::empty();

    size_t id_count = 10;
    for (size_t i = 0;; i++) {
      e.id = i % id_count;
      e.time++;
      e.value = dariadb::Value(i);
      BOOST_CHECK(cap.append(e).writed == 1);

      if (stor->writed_count != 0) {
        break;
      }
    }
    cap.flush();

    // TODO add check: check count of dropped values
    // BOOST_CHECK_EQUAL(cap.size(), size_t(1));

    for (auto it = stor->mlist.cbegin(); it != stor->mlist.cend(); ++it) {
      auto next = it;
      ++next;
      if (next == stor->mlist.cend()) {
        break;
      }

      BOOST_CHECK_GE(next->time, it->time);
    }
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(CapReadIntervalTest) {
  std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
  stor->writed_count = 0;
  const size_t block_size = 10;
  auto storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  {
    auto p = dariadb::storage::Capacitor::Params(block_size, storage_path);
    p.max_levels = 11;

    dariadb::storage::Capacitor cap(stor.get(), p);

    dariadb_test::readIntervalCommonTest(&cap);
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

std::atomic_size_t append_count{0};

void thread_writer(dariadb::Id id, dariadb::Time from, dariadb::Time to,
                   dariadb::Time step, dariadb::storage::Capacitor *cp) {
  const size_t copies_count = 1;
  auto m = dariadb::Meas::empty();
  m.time = from;
  for (auto i = from; i < to; i += step) {
    m.id = id;
    m.flag = dariadb::Flag(i);
    m.value = 0;
    for (size_t j = 0; j < copies_count; j++) {
      ++m.time;
      m.value = dariadb::Value(j);
      cp->append(m);
      append_count++;
    }
  }
}

BOOST_AUTO_TEST_CASE(MultiThread) {
  std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
  stor->writed_count = 0;
  const std::string storage_path = "testStorage";
  const size_t max_size = 10;
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  {
    dariadb::storage::Capacitor mbucket{
        stor.get(), dariadb::storage::Capacitor::Params(max_size, storage_path)};

    std::thread t1(thread_writer, 0, 0, 10, 1, &mbucket);
    std::thread t2(thread_writer, 1, 0, 10, 1, &mbucket);
    std::thread t3(thread_writer, 2, 0, 100, 2, &mbucket);
    std::thread t4(thread_writer, 3, 0, 100, 1, &mbucket);

    t1.join();
    t2.join();
    t3.join();
    t4.join();

    mbucket.flush();
    dariadb::Meas::MeasList out;
    dariadb::IdArray all_id{0, 1, 2, 3};
    dariadb::storage::QueryInterval qi(all_id, 0, 0, 100);
    mbucket.readInterval(qi)->readAll(&out);
    BOOST_CHECK_EQUAL(out.size(), append_count);
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(byStep) {
  std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
  stor->writed_count = 0;
  const size_t max_size = 10;
  auto storage_path = "testStorage";

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  const size_t id_count = 1;
  { // equal step
    dariadb::storage::Capacitor ms{
        stor.get(), dariadb::storage::Capacitor::Params(max_size, storage_path)};

    auto m = dariadb::Meas::empty();
    const size_t total_count = 100;
    const dariadb::Time time_step = 1;
    dariadb::IdSet all_id;
    for (size_t i = 0; i < total_count; i += time_step) {
      m.id = i % id_count;
      all_id.insert(m.id);
      m.flag = dariadb::Flag(i);
      m.time = i;
      m.value = 0;
      ms.append(m);
    }
    dariadb::storage::QueryInterval q_all(dariadb::IdArray{all_id.begin(), all_id.end()},
                                          0, 0, total_count);
    auto rdr = ms.readInterval(q_all);

    dariadb::Meas::MeasList allByStep;
    rdr = ms.readInterval(q_all);
    rdr->readByStep(&allByStep, 0, total_count, time_step);
    auto expected = size_t(total_count / time_step) * id_count; //+ timepoint
    BOOST_CHECK_EQUAL(allByStep.size(), expected);
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  { // less step
    dariadb::storage::Capacitor ms{
        stor.get(), dariadb::storage::Capacitor::Params(max_size, storage_path)};

    auto m = dariadb::Meas::empty();
    const size_t total_count = 100;
    const dariadb::Time time_step = 10;
    dariadb::IdSet all_id;
    for (size_t i = 0; i < total_count; i += time_step) {
      m.id = i % id_count;
      all_id.insert(m.id);
      m.flag = dariadb::Flag(i);
      m.time = i;
      m.value = 0;
      ms.append(m);
    }

    dariadb::storage::QueryInterval q_all(dariadb::IdArray{all_id.begin(), all_id.end()},
                                          0, 0, total_count);
    auto rdr = ms.readInterval(q_all);

    dariadb::Time query_step = 11;
    dariadb::Meas::MeasList allByStep;
    rdr = ms.readInterval(q_all);
    rdr->readByStep(&allByStep, 0, total_count, query_step);
    auto expected = size_t(total_count / query_step) * id_count + id_count; //+ timepoint;
    BOOST_CHECK_EQUAL(allByStep.size(), expected);
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  { // great step
    dariadb::storage::Capacitor ms{
        stor.get(), dariadb::storage::Capacitor::Params(max_size, storage_path)};

    auto m = dariadb::Meas::empty();
    const size_t total_count = 100;
    const dariadb::Time time_step = 10;
    dariadb::IdSet all_id;

    for (size_t i = 0; i < total_count; i += time_step) {
      m.id = i % id_count;
      all_id.insert(m.id);
      m.flag = dariadb::Flag(i);
      m.time = i;
      m.value = 0;
      ms.append(m);
    }
    dariadb::storage::QueryInterval q_all(dariadb::IdArray{all_id.begin(), all_id.end()},
                                          0, 0, total_count);
    auto rdr = ms.readInterval(q_all);
    dariadb::Meas::MeasList all;
    rdr->readAll(&all);

    dariadb::Time query_step = 5;
    dariadb::Meas::MeasList allByStep;
    rdr = ms.readInterval(q_all);
    rdr->readByStep(&allByStep, 0, total_count, query_step);
    auto expected =
        size_t(total_count / time_step) * 2 * id_count + id_count; //+ timepoint;
    BOOST_CHECK_EQUAL(allByStep.size(), expected);
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  { // from before data
    dariadb::storage::Capacitor ms{
        stor.get(), dariadb::storage::Capacitor::Params(max_size, storage_path)};

    auto m = dariadb::Meas::empty();
    const size_t total_count = 100;
    const dariadb::Time time_step = 10;
    dariadb::IdSet all_id;
    for (size_t i = time_step; i < total_count; i += time_step) {
      m.id = i % id_count;
      all_id.insert(m.id);
      m.flag = dariadb::Flag(i);
      m.time = i;
      m.value = 0;
      ms.append(m);
    }
    dariadb::storage::QueryInterval q_all(dariadb::IdArray{all_id.begin(), all_id.end()},
                                          0, time_step, total_count);
    auto rdr = ms.readInterval(q_all);
    dariadb::Meas::MeasList all;
    rdr->readAll(&all);

    dariadb::Time query_step = 5;
    dariadb::Meas::MeasList allByStep;
    rdr = ms.readInterval(q_all);

    rdr->readByStep(&allByStep, 0, total_count, query_step);

    dariadb::Time expected = dariadb::Time((total_count - time_step) / time_step) * 2;
    expected = expected * id_count;
    expected += id_count * (time_step / query_step); //+ before first value
    expected += id_count;                            // one after last  value

    BOOST_CHECK_EQUAL(allByStep.size(), expected);
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

class Moc_I1 : public dariadb::statistic::BaseMethod {
public:
  Moc_I1() { _a = _b = dariadb::Meas::empty(); }
  void calc(const dariadb::Meas &a, const dariadb::Meas &b) override {
    _a = a;
    _b = b;
  }
  dariadb::Value result() const override { return dariadb::Value(); }
  dariadb::Meas _a;
  dariadb::Meas _b;
};

BOOST_AUTO_TEST_CASE(CallCalc) {
  std::unique_ptr<Moc_I1> p{new Moc_I1};
  auto m = dariadb::Meas::empty();
  m.time = 1;
  p->call(m);
  BOOST_CHECK_EQUAL(p->_a.time, dariadb::Time(0));
  m.time = 2;

  p->call(m);
  BOOST_CHECK_EQUAL(p->_a.time, dariadb::Time(1));
  BOOST_CHECK_EQUAL(p->_b.time, dariadb::Time(2));

  m.time = 3;
  p->call(m);
  BOOST_CHECK_EQUAL(p->_a.time, dariadb::Time(2));
  BOOST_CHECK_EQUAL(p->_b.time, dariadb::Time(3));
  std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
  stor->writed_count = 0;
  const size_t max_size = 10;
  auto storage_path = "testStorage";

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  { // equal step
    dariadb::storage::Capacitor ms{
        stor.get(), dariadb::storage::Capacitor::Params(max_size, storage_path)};

    using dariadb::statistic::average::Average;

    m = dariadb::Meas::empty();
    const size_t total_count = 100;
    const dariadb::Time time_step = 1;
    dariadb::IdSet all_id;
    for (size_t i = 0; i < total_count; i += time_step) {
      m.id = 1;
      all_id.insert(m.id);
      m.flag = dariadb::Flag(i);
      m.time = i;
      m.value = 5;
      ms.append(m);
    }

    ms.flush();
    std::unique_ptr<Average> p_average{new Average()};

    dariadb::storage::QueryInterval q_all(dariadb::IdArray{all_id.begin(), all_id.end()},
                                          0, 0, total_count);

    auto rdr = ms.readInterval(q_all);
    /*dariadb::Meas::MeasList ml;
    rdr->readAll(&ml);*/
    p_average->fromReader(rdr, 0, total_count, 1);
    BOOST_CHECK_CLOSE(p_average->result(), dariadb::Value(5), 0.1);
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}
