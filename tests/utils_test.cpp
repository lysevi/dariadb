#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <libdariadb/utils/async/thread_pool.h>
#include <libdariadb/utils/bitoperations.h>
#include <libdariadb/utils/crc.h>
#include <libdariadb/utils/cz.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/in_interval.h>
#include <libdariadb/utils/strings.h>
#include <libdariadb/utils/striped_map.h>
#include <libdariadb/utils/utils.h>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <ctime>
#include <iostream>
#include <thread>

BOOST_AUTO_TEST_CASE(StrippedMapTest) {
  std::cout << "StrippedMapTest" << std::endl;
  using namespace dariadb::utils;

  {
    stripped_map<int, uint64_t> default_ctor;
    BOOST_CHECK_EQUAL(default_ctor.size(), size_t(0));
  }

  {
    stripped_map<int, uint64_t> add;
    add.insert(int(1), uint64_t(2));
    add.insert(int(1), uint64_t(3));
    BOOST_CHECK_EQUAL(add.size(), size_t(1));
    uint64_t output = 0;
    BOOST_CHECK(add.find(int(1), &output));
    BOOST_CHECK_EQUAL(output, uint64_t(3));
  }
  {

    stripped_map<int, uint64_t> add;
    {
      auto iter = add.insertion_pos(1);
      iter.v->second = uint64_t(3);
    }
    uint64_t output = 0;
    BOOST_CHECK(add.find(1, &output));
    BOOST_CHECK_EQUAL(output, uint64_t(3));
  }

  {
    stripped_map<int, uint64_t> add_many;
    int key = 0;
    uint64_t value = 0;
    while (add_many.N() == add_many.default_n) {
      auto k = key++;
      auto v = value++;
      add_many.insert(k, v);
    }

    for (int for_search = 0; for_search < key; ++for_search) {
      uint64_t output = 0;
      if (add_many.find(for_search, &output)) {
        BOOST_CHECK_EQUAL(output, uint64_t(for_search));
      } else {
        BOOST_CHECK_MESSAGE(false, "key=" << for_search << " not found");
      }
    }
    size_t cnt = 0;
    auto f = [&cnt](const stripped_map<int, uint64_t>::value_type &kv) { cnt++; };
    add_many.apply(f);
    BOOST_CHECK_EQUAL(add_many.size(), size_t(key));
    BOOST_CHECK_EQUAL(add_many.size(), cnt);
  }
}

BOOST_AUTO_TEST_CASE(TimeToString) {
  std::cout << "TimeToString" << std::endl;
  auto ct = dariadb::timeutil::current_time();
  BOOST_CHECK(ct != dariadb::Time(0));
  auto ct_str = dariadb::timeutil::to_string(ct);
  BOOST_CHECK(ct_str.size() != 0);
}

BOOST_AUTO_TEST_CASE(TimeRound) {
  std::cout << "TimeRound" << std::endl;
  auto ct = dariadb::timeutil::current_time();
  {
    auto rounded = dariadb::timeutil::round_to_seconds(ct);
    auto rounded_d = dariadb::timeutil::to_datetime(rounded);
    BOOST_CHECK_EQUAL(rounded_d.millisecond, uint16_t(0));
  }

  {
    auto rounded = dariadb::timeutil::round_to_minutes(ct);
    auto rounded_d = dariadb::timeutil::to_datetime(rounded);
    BOOST_CHECK_EQUAL(rounded_d.millisecond, uint16_t(0));
    BOOST_CHECK_EQUAL(rounded_d.second, uint16_t(0));
  }

  {
    auto rounded = dariadb::timeutil::round_to_hours(ct);
    auto rounded_d = dariadb::timeutil::to_datetime(rounded);
    BOOST_CHECK_EQUAL(rounded_d.millisecond, uint16_t(0));
    BOOST_CHECK_EQUAL(rounded_d.second, uint16_t(0));
    BOOST_CHECK_EQUAL(rounded_d.minute, uint16_t(0));
  }
}

BOOST_AUTO_TEST_CASE(CRC32Test) {
  std::cout << "CRC32Test" << std::endl;
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
  std::cout << "CRC16Test" << std::endl;
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

BOOST_AUTO_TEST_CASE(CountZero) {
  std::cout << "CountZero" << std::endl;
  BOOST_CHECK_EQUAL(dariadb::utils::clz(67553994410557440), 8);
  BOOST_CHECK_EQUAL(dariadb::utils::clz(3458764513820540928), 2);
  BOOST_CHECK_EQUAL(dariadb::utils::clz(15), 60);

  BOOST_CHECK_EQUAL(dariadb::utils::ctz(240), 4);
  BOOST_CHECK_EQUAL(dariadb::utils::ctz(3840), 8);
}

BOOST_AUTO_TEST_CASE(InInterval) {
  std::cout << "InInterval" << std::endl;
  BOOST_CHECK(dariadb::utils::inInterval(1, 5, 1));
  BOOST_CHECK(dariadb::utils::inInterval(1, 5, 2));
  BOOST_CHECK(dariadb::utils::inInterval(1, 5, 5));
  BOOST_CHECK(!dariadb::utils::inInterval(1, 5, 0));
  BOOST_CHECK(!dariadb::utils::inInterval(0, 1, 2));
}

BOOST_AUTO_TEST_CASE(BitOperations) {
  std::cout << "BitOperations" << std::endl;
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
  std::cout << "FileUtils" << std::endl;
  std::string filename = "foo/bar/test.txt";
  BOOST_CHECK_EQUAL(dariadb::utils::fs::filename(filename), "test");
  BOOST_CHECK_EQUAL(dariadb::utils::fs::parent_path(filename), "foo/bar");
  BOOST_CHECK_EQUAL(dariadb::utils::fs::extract_filename(filename), "test.txt");

  auto ls_res = dariadb::utils::fs::ls(".");
  BOOST_CHECK(ls_res.size() > 0);

  std::string parent_p = "path1";
  std::string child_p = "path2";
  auto concat_p = dariadb::utils::fs::append_path(parent_p, child_p);
  BOOST_CHECK_EQUAL(dariadb::utils::fs::parent_path(concat_p), parent_p);
}

BOOST_AUTO_TEST_CASE(ThreadsPool) {
  std::cout << "ThreadsPool" << std::endl;
  using namespace dariadb::utils::async;

  const ThreadKind tk = 1;
  {
    const size_t threads_count = 2;
    ThreadPool tp(ThreadPool::Params(threads_count, tk));

    BOOST_CHECK_EQUAL(tp.threads_count(), threads_count);
    BOOST_CHECK(!tp.isStoped());
    tp.stop();
    BOOST_CHECK(tp.isStoped());
  }

  {
    const size_t threads_count = 2;
    ThreadPool tp(ThreadPool::Params(threads_count, tk));
    const size_t tasks_count = 100;
    AsyncTask at = [tk](const ThreadInfo &ti) {
      if (tk != ti.kind) {
        BOOST_TEST_MESSAGE("(tk != ti.kind)");
        throw MAKE_EXCEPTION("(tk != ti.kind)");
      }
      return false;
    };
    for (size_t i = 0; i < tasks_count; ++i) {
      tp.post(AT(at));
    }
    tp.flush();

    auto lock = tp.post(AT(at));
    lock->wait();

    tp.stop();
  }

  { // without flush
    const size_t threads_count = 2;
    ThreadPool tp(ThreadPool::Params(threads_count, tk));
    const size_t tasks_count = 100;
    AsyncTask at = [tk](const ThreadInfo &ti) {
      if (tk != ti.kind) {
        BOOST_TEST_MESSAGE("(tk != ti.kind)");
        throw MAKE_EXCEPTION("(tk != ti.kind)");
      }
      return false;
    };
    for (size_t i = 0; i < tasks_count; ++i) {
      tp.post(AT(at));
    }

    tp.stop();
  }
}

BOOST_AUTO_TEST_CASE(ThreadsManager) {
  std::cout << "ThreadsManager" << std::endl;
  using namespace dariadb::utils::async;

  const ThreadKind tk1 = 1;
  const ThreadKind tk2 = 2;
  size_t threads_count = 2;
  ThreadPool::Params tp1(threads_count, tk1);
  ThreadPool::Params tp2(threads_count, tk2);

  ThreadManager::Params tpm_params(std::vector<ThreadPool::Params>{tp1, tp2});
  {
    BOOST_CHECK(ThreadManager::instance() == nullptr);
    ThreadManager::start(tpm_params);
    BOOST_CHECK(ThreadManager::instance() != nullptr);
    ThreadManager::stop();
    BOOST_CHECK(ThreadManager::instance() == nullptr);
  }

  {
    const size_t tasks_count = 10;
    ThreadManager::start(tpm_params);
    int called = 0;
    uint64_t inf_calls = 0;
    AsyncTask infinite_worker = [&inf_calls](const ThreadInfo &) {
      ++inf_calls;
      return true;
    };

    AsyncTask at_while = [&called](const ThreadInfo &) {
      if (called < 10) {
        ++called;
        return true;
      }
      return false;
    };
    AsyncTask at1 = [tk1](const ThreadInfo &ti) {
      if (tk1 != ti.kind) {
        BOOST_TEST_MESSAGE("(tk1 != ti.kind)");
        dariadb::utils::sleep_mls(400);
        throw MAKE_EXCEPTION("(tk1 != ti.kind)");
      }
      return false;
    };
    AsyncTask at2 = [tk2](const ThreadInfo &ti) {
      if (tk2 != ti.kind) {
        BOOST_TEST_MESSAGE("(tk2 != ti.kind)");
        dariadb::utils::sleep_mls(400);
        throw MAKE_EXCEPTION("(tk2 != ti.kind)");
      }
      return false;
    };
    ThreadManager::instance()->post(
        tk1, AT_PRIORITY(infinite_worker, dariadb::utils::async::TASK_PRIORITY::WORKER));
    auto at_while_res = ThreadManager::instance()->post(tk1, AT(at_while));
    for (size_t i = 0; i < tasks_count; ++i) {
      ThreadManager::instance()->post(tk1, AT(at1));
      ThreadManager::instance()->post(tk2, AT(at2));
    }
    BOOST_CHECK_GE(ThreadManager::instance()->active_works(), size_t(0));
    at_while_res->wait();
    BOOST_CHECK_EQUAL(called, int(10));
    ThreadManager::instance()->flush();
    ThreadManager::instance()->stop();
  }
}

BOOST_AUTO_TEST_CASE(SplitString) {
  std::cout << "SplitString" << std::endl;
  std::string str = "1 2 3 4 5 6 7 8";
  auto splitted = dariadb::utils::strings::tokens(str);
  BOOST_CHECK_EQUAL(splitted.size(), size_t(8));

  splitted = dariadb::utils::strings::split(str, ' ');
  BOOST_CHECK_EQUAL(splitted.size(), size_t(8));
}
