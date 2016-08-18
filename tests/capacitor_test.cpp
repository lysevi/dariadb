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
#include <storage/capacitor_manager.h>
#include <storage/manifest.h>
#include <storage/options.h>
#include <timeutil.h>
#include <utils/exception.h>
#include <utils/fs.h>
#include <utils/logger.h>
#include <utils/thread_manager.h>

class Moc_Dropper : public dariadb::storage::ICapDropper {
public:
  size_t calls;
  Moc_Dropper() {
    calls = 0;
    max_time = dariadb::MIN_TIME;
  }
  virtual void drop_cap(const std::string &f) override {
    calls++;
    auto hdr = dariadb::storage::Capacitor::readHeader(f);
    max_time = std::max(hdr.maxTime, max_time);
  }
  dariadb::Time max_time;
};

BOOST_AUTO_TEST_CASE(CapacitorInitTest) {
  const size_t block_size = 10;
  auto storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  dariadb::utils::fs::mkdir(storage_path);

  auto cap_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::CAP_FILE_EXT);
  assert(cap_files.size() == 0);
  dariadb::utils::LogManager::start();
  dariadb::storage::Options::start();
  dariadb::storage::Options::instance()->path = storage_path;
  dariadb::storage::Options::instance()->cap_B = block_size;
  dariadb::storage::Options::instance()->cap_max_levels = 11;

  size_t writes_count = 10000;

  dariadb::IdSet id_set;
  {
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storage_path, "Manifest"));
    dariadb::storage::Capacitor cap(dariadb::storage::Capacitor::rnd_file_name());

    cap_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::CAP_FILE_EXT);
    BOOST_CHECK_EQUAL(cap_files.size(), size_t(1));

    BOOST_CHECK_EQUAL(cap.levels_count(),
                      dariadb::storage::Options::instance()->cap_max_levels);

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

    out = cap.readInterval(dariadb::storage::QueryInterval(
        dariadb::IdArray(id_set.begin(), id_set.end()), 0, 0, writes_count));

    BOOST_CHECK_EQUAL(out.size(), cap.size());

    dariadb::storage::Manifest::stop();
  }
  {
    dariadb::storage::Options::instance()->cap_max_levels = 12;
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storage_path, "Manifest"));
    auto fname = dariadb::utils::fs::append_path(
        storage_path, dariadb::storage::Manifest::instance()->cola_list().front());
    dariadb::storage::Capacitor cap(fname);

    cap_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::CAP_FILE_EXT);
    BOOST_CHECK_EQUAL(cap_files.size(), size_t(1));
    if (cap_files.size() != size_t(1)) {
      for (auto f : cap_files) {
        std::cout << ">> " << f << std::endl;
      }
      auto cl = dariadb::storage::Manifest::instance()->cola_list();
      for (auto f : cl) {
        std::cout << "** " << f << std::endl;
      }
    }
    auto cap_size = cap.size();
    // level count must be read from file header.
    BOOST_CHECK_LE(cap.levels_count(),
                   dariadb::storage::Options::instance()->cap_max_levels);

    BOOST_CHECK_EQUAL(cap_size, writes_count);
    BOOST_CHECK(cap.size() != 0);
    auto e = dariadb::Meas::empty();

    e.time = writes_count - 1;
    BOOST_CHECK(cap.append(e).writed == 1);

    dariadb::Meas::MeasList out;
    auto ids = dariadb::IdArray(id_set.begin(), id_set.end());
    dariadb::storage::QueryInterval qi{ids, 0, 0, writes_count};
    out = cap.readInterval(qi);

    BOOST_CHECK_EQUAL(out.size(), cap.size());
    BOOST_CHECK_LT(cap_size, cap.size());
    dariadb::storage::Manifest::stop();
  }

  dariadb::storage::Options::stop();
  dariadb::utils::LogManager::stop();
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(CapacitorCommonTest) {
  const size_t block_size = 10;
  auto storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  dariadb::utils::fs::mkdir(storage_path);
  dariadb::utils::LogManager::start();
  {
    auto cap_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::CAP_FILE_EXT);
    assert(cap_files.size() == 0);
    dariadb::storage::Options::start();
    dariadb::storage::Options::instance()->path = storage_path;
    dariadb::storage::Options::instance()->cap_B = block_size;
    dariadb::storage::Options::instance()->cap_max_levels = 11;

    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storage_path, "Manifest"));
    dariadb::storage::Capacitor cap(dariadb::storage::Capacitor::rnd_file_name());

    dariadb_test::storage_test_check(&cap, 0, 100, 1);
    dariadb::storage::Manifest::stop();
  }
  dariadb::storage::Options::stop();
  dariadb::utils::LogManager::stop();
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(CapacitorIsFull) {
  const size_t block_size = 10;
  auto storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  dariadb::utils::fs::mkdir(storage_path);
  dariadb::utils::LogManager::start();
  auto cap_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::CAP_FILE_EXT);
  assert(cap_files.size() == 0);
  dariadb::storage::Options::start();
  dariadb::storage::Options::instance()->path = storage_path;
  dariadb::storage::Options::instance()->cap_B = block_size;
  dariadb::storage::Options::instance()->cap_max_levels = 11;
  {
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storage_path, "Manifest"));
    dariadb::storage::Capacitor cap(dariadb::storage::Capacitor::rnd_file_name());

    auto e = dariadb::Meas::empty();

    size_t id_count = 10;
    size_t addeded = 0;
    for (size_t i = 0;; i++) {
      e.id = i % id_count;
      e.time++;
      e.value = dariadb::Value(i);
      if (cap.append(e).ignored != 0) {
        break;
      }
      addeded++;
    }
    BOOST_CHECK_GT(addeded, size_t(0));

    auto mc = dariadb::storage::Options::instance()->measurements_count();
    BOOST_CHECK_EQUAL(addeded, mc);
    auto all = cap.readAll();
    BOOST_CHECK_EQUAL(addeded, all.size());
  }
  auto clist = dariadb::storage::Manifest::instance()->cola_list();
  BOOST_CHECK_EQUAL(clist.size(), size_t(1));
  auto hdr = dariadb::storage::Capacitor::readHeader(
      dariadb::utils::fs::append_path(storage_path, clist.front()));
  BOOST_CHECK(hdr.is_closed = true);
  BOOST_CHECK(hdr.is_full = true);

  dariadb::storage::Manifest::stop();
  dariadb::storage::Options::stop();
  dariadb::utils::LogManager::stop();
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(CapacitorBulk) {
  const size_t block_size = 10;
  auto storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  dariadb::utils::fs::mkdir(storage_path);

  auto cap_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::CAP_FILE_EXT);
  assert(cap_files.size() == 0);
  dariadb::utils::LogManager::start();
  dariadb::storage::Options::start();
  dariadb::storage::Options::instance()->path = storage_path;
  dariadb::storage::Options::instance()->cap_B = block_size;
  dariadb::storage::Options::instance()->cap_max_levels = 11;
  {
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storage_path, "Manifest"));
    dariadb::storage::Capacitor cap(dariadb::storage::Capacitor::rnd_file_name());

    auto e = dariadb::Meas::empty();
    size_t count = dariadb::storage::Options::instance()->measurements_count();
    dariadb::Meas::MeasArray a(count);
    for (size_t i = 0; i < count; i++) {
      e.id = 0;
      e.time++;
      e.value = dariadb::Value(i);
      a[i] = e;
    }
    cap.append(a.begin(), a.end());

    auto values = cap.readInterval(dariadb::storage::QueryInterval({0}, 0, 0, e.time));
    BOOST_CHECK_EQUAL(values.size(), count);
  }

  dariadb::storage::Manifest::stop();
  dariadb::storage::Options::stop();
  dariadb::utils::LogManager::stop();
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

// BOOST_AUTO_TEST_CASE(CapReadIntervalTest) {
//  std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
//  stor->writed_count = 0;
//  const size_t block_size = 10;
//  auto storage_path = "testStorage";
//  if (dariadb::utils::fs::path_exists(storage_path)) {
//    dariadb::utils::fs::rm(storage_path);
//  }
//  dariadb::utils::fs::mkdir(storage_path);
//  {
//    auto p = dariadb::storage::Capacitor::Params(block_size, storage_path);
//    p.max_levels = 11;
//    dariadb::storage::Manifest::start(
//        dariadb::utils::fs::append_path(storage_path, "Manifest"));
//    dariadb::storage::Capacitor cap(p, dariadb::storage::Capacitor::file_name());
//
//    dariadb_test::readIntervalCommonTest(&cap);
//    dariadb::storage::Manifest::stop();
//  }
//
//  if (dariadb::utils::fs::path_exists(storage_path)) {
//    dariadb::utils::fs::rm(storage_path);
//  }
//}

/*
BOOST_AUTO_TEST_CASE(byStep) {
  const size_t max_size = 10;
  auto storage_path = "testStorage";

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  dariadb::utils::fs::mkdir(storage_path);
  const size_t id_count = 1;
  { // equal step
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storage_path, "Manifest"));
    dariadb::storage::Capacitor ms{
        dariadb::storage::Capacitor::Params(max_size, storage_path),
        dariadb::storage::Capacitor::file_name()};

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
    dariadb::storage::Manifest::stop();
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  dariadb::utils::fs::mkdir(storage_path);
  { // less step
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storage_path, "Manifest"));
    dariadb::storage::Capacitor ms{
        dariadb::storage::Capacitor::Params(max_size, storage_path),
        dariadb::storage::Capacitor::file_name()};

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
    dariadb::storage::Manifest::stop();
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  dariadb::utils::fs::mkdir(storage_path);
  { // great step
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storage_path, "Manifest"));
    dariadb::storage::Capacitor ms{
        dariadb::storage::Capacitor::Params(max_size, storage_path),
        dariadb::storage::Capacitor::file_name()};

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
    dariadb::storage::Manifest::stop();
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  dariadb::utils::fs::mkdir(storage_path);
  { // from before data
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storage_path, "Manifest"));
    dariadb::storage::Capacitor ms{
        dariadb::storage::Capacitor::Params(max_size, storage_path),
        dariadb::storage::Capacitor::file_name()};

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
    dariadb::storage::Manifest::stop();
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

  const size_t max_size = 10;
  auto storage_path = "testStorage";

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  dariadb::utils::fs::mkdir(storage_path);
  { // equal step
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storage_path, "Manifest"));
    dariadb::storage::Capacitor ms{
        dariadb::storage::Capacitor::Params(max_size, storage_path),
        dariadb::storage::Capacitor::file_name()};

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

    p_average->fromReader(rdr, 0, total_count, 1);
    BOOST_CHECK_CLOSE(p_average->result(), dariadb::Value(5), 0.1);
    dariadb::storage::Manifest::stop();
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}
*/
BOOST_AUTO_TEST_CASE(CapManager_Instance) {
  const std::string storagePath = "testStorage";
  const size_t max_size = 10;
  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  dariadb::utils::fs::mkdir(storagePath);
  dariadb::storage::Manifest::start(
      dariadb::utils::fs::append_path(storagePath, "Manifest"));
  dariadb::utils::LogManager::start();
  dariadb::storage::Options::start();
  dariadb::storage::Options::instance()->path = storagePath;
  dariadb::storage::Options::instance()->cap_B = max_size;
  dariadb::storage::Options::instance()->cap_max_levels = max_size;
  dariadb::utils::async::ThreadManager::start(dariadb::storage::Options::instance()->thread_pools_params());

  dariadb::storage::CapacitorManager::start();

  BOOST_CHECK(dariadb::storage::CapacitorManager::instance() != nullptr);

  auto cap_files = dariadb::utils::fs::ls(storagePath, dariadb::storage::CAP_FILE_EXT);
  BOOST_CHECK_EQUAL(cap_files.size(), size_t(0));

  dariadb::storage::CapacitorManager::stop();
  dariadb::storage::Manifest::stop();
  dariadb::storage::Options::stop();
  dariadb::utils::async::ThreadManager::stop();
  dariadb::utils::LogManager::stop();
  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}

BOOST_AUTO_TEST_CASE(CapManager_CommonTest) {
  const std::string storagePath = "testStorage";
  const size_t max_size = 5;
  const dariadb::Time from = 0;
  const dariadb::Time to = from + 1021;
  const dariadb::Time step = 10;

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  dariadb::utils::fs::mkdir(storagePath);
  dariadb::utils::LogManager::start();
  {
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storagePath, "Manifest"));
    dariadb::storage::Options::start();
    dariadb::storage::Options::instance()->path = storagePath;
    dariadb::storage::Options::instance()->cap_B = max_size;
    dariadb::storage::Options::instance()->cap_max_levels = max_size;
    dariadb::utils::async::ThreadManager::start(dariadb::storage::Options::instance()->thread_pools_params());

    dariadb::storage::CapacitorManager::start();

    dariadb_test::storage_test_check(dariadb::storage::CapacitorManager::instance(), from,
                                     to, step);

    dariadb::storage::CapacitorManager::stop();
    dariadb::storage::Manifest::stop();
    dariadb::utils::async::ThreadManager::stop();
    dariadb::storage::Options::stop();
  }
  {
    std::shared_ptr<Moc_Dropper> stor(new Moc_Dropper);

    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storagePath, "Manifest"));
    dariadb::storage::Options::start();
    dariadb::storage::Options::instance()->path = storagePath;
    dariadb::storage::Options::instance()->cap_B = max_size;
    dariadb::storage::Options::instance()->cap_max_levels = max_size;
    dariadb::utils::async::ThreadManager::start(dariadb::storage::Options::instance()->thread_pools_params());
    dariadb::storage::CapacitorManager::start();

    dariadb::storage::QueryInterval qi(dariadb::IdArray{0}, dariadb::Flag(), from, to);

    auto out = dariadb::storage::CapacitorManager::instance()->readInterval(qi);
    BOOST_CHECK_EQUAL(out.size(), dariadb_test::copies_count);

    auto closed = dariadb::storage::CapacitorManager::instance()->closed_caps();
    BOOST_CHECK(closed.size() != size_t(0));
    dariadb::storage::CapacitorManager::instance()->set_downlevel(stor.get());

    for (auto fname : closed) {
      dariadb::storage::CapacitorManager::instance()->drop_cap(fname);
    }
    BOOST_CHECK_EQUAL(stor.get()->calls, closed.size());

    dariadb::storage::CapacitorManager::stop();
    dariadb::storage::Manifest::stop();
    dariadb::utils::async::ThreadManager::stop();
    dariadb::storage::Options::stop();
  }
  dariadb::utils::LogManager::stop();
  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}

BOOST_AUTO_TEST_CASE(CapManagerDropByPeriod) {
  const std::string storagePath = "testStorage";
  const size_t max_size = 5;
  const dariadb::Time from = dariadb::timeutil::current_time();
  const dariadb::Time to = from + 1021;
  const dariadb::Time step = 10;
  const size_t copies_count = 100;
  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  dariadb::utils::fs::mkdir(storagePath);
  dariadb::utils::LogManager::start();
  {
    std::shared_ptr<Moc_Dropper> stor(new Moc_Dropper);

    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storagePath, "Manifest"));
    dariadb::storage::Options::start();
    dariadb::storage::Options::instance()->path = storagePath;
    dariadb::storage::Options::instance()->cap_B = max_size;
    dariadb::storage::Options::instance()->cap_max_levels = max_size;
    dariadb::storage::Options::instance()->cap_max_closed_caps = 0;
    dariadb::storage::Options::instance()->cap_store_period = 1000;
    dariadb::utils::async::ThreadManager::start(dariadb::storage::Options::instance()->thread_pools_params());

    dariadb::storage::CapacitorManager::start();

    dariadb::storage::CapacitorManager::instance()->set_downlevel(stor.get());

    auto m = dariadb::Meas::empty();
    for (auto i = from; i < to; i += step) {
      m.id = 1;
      m.flag = 1;
      m.src = 1;
      m.time = i;
      m.value = 0;

      dariadb::Meas::MeasArray values{copies_count};
      for (size_t j = 1; j < copies_count + 1; j++) {
        m.time++;
        if (dariadb::storage::CapacitorManager::instance()->append(m).ignored != 0) {
          throw MAKE_EXCEPTION("->append(m).writed != values.size()");
        }
      }
    }

    auto current = dariadb::timeutil::current_time();
    BOOST_CHECK(stor->max_time < current);
    dariadb::storage::CapacitorManager::stop();
    dariadb::storage::Manifest::stop();
    dariadb::utils::async::ThreadManager::stop();
    dariadb::storage::Options::stop();
  }
  dariadb::utils::LogManager::stop();
  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}
