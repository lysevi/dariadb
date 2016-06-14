#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <ctime>
#include <thread>
#include <utils/asyncworker.h>
#include <utils/crc.h>
#include <utils/fs.h>
#include <utils/in_interval.h>
#include <utils/lru.h>
#include <utils/period_worker.h>
#include <utils/skiplist.h>
#include <utils/utils.h>

BOOST_AUTO_TEST_CASE(CRC32Test) {
  uint64_t data;
  char *pdata = (char *)&data;
  data = 1;
  auto res1 = dariadb::utils::crc32(pdata, sizeof(data));
  auto res2 = dariadb::utils::crc32(pdata, sizeof(data));
  data++;
  auto res3 = dariadb::utils::crc32(pdata, sizeof(data));
  BOOST_CHECK_EQUAL(res1, res2);
  BOOST_CHECK(res1 != res3);
}

BOOST_AUTO_TEST_CASE(CRC16Test) {
  uint64_t data;
  char *pdata = (char *)&data;
  data = 1;
  auto res1 = dariadb::utils::crc16(pdata, sizeof(data));
  auto res2 = dariadb::utils::crc16(pdata, sizeof(data));
  data++;
  auto res3 = dariadb::utils::crc16(pdata, sizeof(data));
  BOOST_CHECK_EQUAL(res1, res2);
  BOOST_CHECK(res1 != res3);
}

BOOST_AUTO_TEST_CASE(LRUCheck) {
  dariadb::utils::LRU<int, int> ilru(4);

  int out_val;
  BOOST_CHECK(!ilru.put(1, 10, &out_val));
  BOOST_CHECK(!ilru.put(2, 20, &out_val));
  BOOST_CHECK(!ilru.put(3, 30, &out_val));
  BOOST_CHECK(!ilru.put(4, 40, &out_val));
  BOOST_CHECK(ilru.put(5, 50, &out_val));
  BOOST_CHECK_EQUAL(ilru.size(), size_t(4));
  BOOST_CHECK_EQUAL(out_val, 10);

  BOOST_CHECK(!ilru.find(1, &out_val));
  BOOST_CHECK(ilru.find(2, &out_val));
  BOOST_CHECK_EQUAL(out_val, 20);
  BOOST_CHECK(ilru.find(3, &out_val));
  BOOST_CHECK_EQUAL(out_val, 30);
  BOOST_CHECK(ilru.find(4, &out_val));
  BOOST_CHECK_EQUAL(out_val, 40);
  BOOST_CHECK(ilru.find(5, &out_val));
  BOOST_CHECK_EQUAL(out_val, 50);

  ilru.set_max_size(10);
  for (auto i = ilru.size() + 1; i < size_t(11); ++i) {
    auto sub_res = ilru.put(int(i), int(i * 10), &out_val);
    BOOST_CHECK(!sub_res);
  }

  BOOST_CHECK(ilru.put(55, 550, &out_val));
  BOOST_CHECK_EQUAL(ilru.size(), size_t(10));
  BOOST_CHECK_EQUAL(out_val, 20);
}

BOOST_AUTO_TEST_CASE(SkipListCheck) {
  using int_lst = dariadb::utils::skiplist<size_t, size_t>;

  int_lst lst;
  const size_t insertions = 33;
  size_t sum_before = 0;
  for (size_t i = 0; i < insertions; i += 3) {
    lst.insert(i, i * 5);
    lst.insert(i + 2, (i + 2) * 10);
    lst.insert(i + 1, (i + 1) * 10);
    lst.insert(i, i * 10);
    sum_before += i + i + 2 + i + 1;
  }
  // lst.print();
  BOOST_CHECK_EQUAL(lst.size(), size_t(33));
  for (auto &kv : lst) {
    kv.second = kv.first * 11;
  }
  lst.remove(0);
  BOOST_CHECK_EQUAL(lst.size(), size_t(32));
  for (size_t i = 0; i < 33; ++i) {
    auto fpos = lst.find(i);
    if (fpos != lst.end()) {
      auto kv = *fpos;
      BOOST_CHECK_EQUAL(kv.second, i * 11);
    } else {
      if (i != 0) { // we remove 0;
        BOOST_CHECK(fpos != lst.end());
      }
    }
  }
  size_t sum = 0;
  for (auto it = lst.begin(); it != lst.end(); ++it) {
    sum += it->first;
  }
  BOOST_CHECK_EQUAL(sum, sum_before);

  sum = 0;
  for (auto it = lst.cbegin(); it != lst.cend(); ++it) {
    sum += it->first;
  }
  BOOST_CHECK_EQUAL(sum, sum_before);

  lst.remove(size_t(3));
  // lst.print();
  BOOST_CHECK(lst.find(size_t(3)) == lst.end());

  auto ub = lst.upper_bound(size_t(4));
  BOOST_CHECK_EQUAL(ub->first, size_t(5));

  auto lb = lst.lower_bound(size_t(4));
  BOOST_CHECK_EQUAL(lb->first, size_t(2));

  lb = lst.lower_bound(size_t(3));
  BOOST_CHECK_EQUAL(lb->first, size_t(2));

  lst.remove_if(lst.begin(), lst.end(),
                [](const int_lst::pair_type &kv) { return kv.first < 20; });
  for (auto it : lst) {
    BOOST_CHECK(it.first >= 20);
  }
  // lst.print();
}

BOOST_AUTO_TEST_CASE(CountZero) {
  BOOST_CHECK_EQUAL(dariadb::utils::clz(67553994410557440), 8);
  BOOST_CHECK_EQUAL(dariadb::utils::clz(3458764513820540928), 2);
  BOOST_CHECK_EQUAL(dariadb::utils::clz(15), 60);

  BOOST_CHECK_EQUAL(dariadb::utils::ctz(240), 4);
  BOOST_CHECK_EQUAL(dariadb::utils::ctz(3840), 8);
}

BOOST_AUTO_TEST_CASE(InInterval) {
  BOOST_CHECK(dariadb::utils::inInterval(1, 5, 1));
  BOOST_CHECK(dariadb::utils::inInterval(1, 5, 2));
  BOOST_CHECK(dariadb::utils::inInterval(1, 5, 5));
  BOOST_CHECK(!dariadb::utils::inInterval(1, 5, 0));
  BOOST_CHECK(!dariadb::utils::inInterval(0, 1, 2));
}

BOOST_AUTO_TEST_CASE(BitOperations) {
  uint8_t value = 0;
  for (int8_t i = 0; i < 7; i++) {
    value = dariadb::utils::BitOperations::set(value, i);
    BOOST_CHECK_EQUAL(dariadb::utils::BitOperations::check(value, i), true);
  }

  for (int8_t i = 0; i < 7; i++) {
    value = dariadb::utils::BitOperations::clr(value, i);
  }

  for (int8_t i = 0; i < 7; i++) {
    BOOST_CHECK_EQUAL(dariadb::utils::BitOperations::check(value, i), false);
  }
}

BOOST_AUTO_TEST_CASE(FileUtils) {
  std::string filename = "foo/bar/test.txt";
  BOOST_CHECK_EQUAL(dariadb::utils::fs::filename(filename), "test");
  BOOST_CHECK_EQUAL(dariadb::utils::fs::parent_path(filename), "foo/bar");

  auto ls_res = dariadb::utils::fs::ls(".");
  BOOST_CHECK(ls_res.size() > 0);

  const std::string fname = "mapped_file.test";
  auto mapf = dariadb::utils::fs::MappedFile::touch(fname, 1024);
  for (uint8_t i = 0; i < 100; i++) {
    mapf->data()[i] = i;
  }
  mapf->close();

  ls_res = dariadb::utils::fs::ls(".", ".test");
  BOOST_CHECK(ls_res.size() == 1);
  auto reopen_mapf = dariadb::utils::fs::MappedFile::open(fname);
  for (uint8_t i = 0; i < 100; i++) {
    BOOST_CHECK_EQUAL(reopen_mapf->data()[i], i);
  }
  reopen_mapf->close();
  dariadb::utils::fs::rm(fname);

  std::string parent_p = "path1";
  std::string child_p = "path2";
  auto concat_p = dariadb::utils::fs::append_path(parent_p, child_p);
  BOOST_CHECK_EQUAL(dariadb::utils::fs::parent_path(concat_p), parent_p);
}

class TestPeriodWorker : public dariadb::utils::PeriodWorker {
public:
  TestPeriodWorker(const std::chrono::milliseconds sleep_time)
      : dariadb::utils::PeriodWorker(sleep_time) {
    call_count = 0;
  }
  void call() { call_count++; }
  size_t call_count;
};

BOOST_AUTO_TEST_CASE(PeriodWorkerTest) {
  auto secs_1 = std::chrono::milliseconds(1000);
  auto secs_3 = std::chrono::milliseconds(1300);
  std::unique_ptr<TestPeriodWorker> worker{new TestPeriodWorker(secs_1)};
  worker->start_worker();
  std::this_thread::sleep_for(secs_3);
  worker->stop_worker();
  BOOST_CHECK(worker->call_count > 1);
}

class TestWorker : public dariadb::utils::AsyncWorker<int> {
public:
  int value;
  TestWorker() : value(0) {}
  void call_async(const int &data) override { value += data; }
};

BOOST_AUTO_TEST_CASE(Worker) {

  TestWorker worker;

  worker.start_async();

  BOOST_CHECK(!worker.is_busy());

  worker.add_async_data(1);
  worker.add_async_data(2);
  worker.add_async_data(3);
  worker.add_async_data(4);

  worker.flush_async();

  BOOST_CHECK(!worker.is_busy());

  worker.stop_async();
  BOOST_CHECK(worker.stoped());
  BOOST_CHECK_EQUAL(worker.value, (int)1 + 2 + 3 + 4);
}
