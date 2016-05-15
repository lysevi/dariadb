#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <atomic>
#include <boost/test/unit_test.hpp>
#include <cassert>
#include <map>
#include <thread>

#include "test_common.h"
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

  auto cap_files =
      dariadb::utils::fs::ls(storage_path, dariadb::storage::CAP_FILE_EXT);
  assert(cap_files.size() == 0);
  auto p = dariadb::storage::Capacitor::Params(block_size, storage_path);
  p.max_levels = 11;
  size_t writes_count = 10000;

  dariadb::IdSet id_set;
  {

    dariadb::storage::Capacitor cap(stor.get(), p);

    cap_files =
        dariadb::utils::fs::ls(storage_path, dariadb::storage::CAP_FILE_EXT);
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

    cap_files =
        dariadb::utils::fs::ls(storage_path, dariadb::storage::CAP_FILE_EXT);
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
    auto cap_files =
        dariadb::utils::fs::ls(storage_path, dariadb::storage::CAP_FILE_EXT);
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
    auto cap_files =
        dariadb::utils::fs::ls(storage_path, dariadb::storage::CAP_FILE_EXT);
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
        stor.get(),
        dariadb::storage::Capacitor::Params(max_size, storage_path)};

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
