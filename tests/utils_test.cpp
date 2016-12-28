#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <ctime>
#include <ctime>
#include <iostream>
#include <thread>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/cz.h>
#include <libdariadb/utils/crc.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/in_interval.h>
#include <libdariadb/utils/metrics.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <libdariadb/utils/async/thread_pool.h>
#include <libdariadb/utils/utils.h>
#include <libdariadb/utils/strings.h>

BOOST_AUTO_TEST_CASE(TimeToString) {
  auto ct = dariadb::timeutil::current_time();
  BOOST_CHECK(ct != dariadb::Time(0));
  auto ct_str = dariadb::timeutil::to_string(ct);
  BOOST_CHECK(ct_str.size() != 0);
}

BOOST_AUTO_TEST_CASE(TimeRound) {
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
  BOOST_CHECK_EQUAL(dariadb::utils::fs::extract_filename(filename), "test.txt");

  auto ls_res = dariadb::utils::fs::ls(".");
  BOOST_CHECK(ls_res.size() > 0);

  std::string parent_p = "path1";
  std::string child_p = "path2";
  auto concat_p = dariadb::utils::fs::append_path(parent_p, child_p);
  BOOST_CHECK_EQUAL(dariadb::utils::fs::parent_path(concat_p), parent_p);
}


BOOST_AUTO_TEST_CASE(Metrics) {
  BOOST_CHECK(dariadb::utils::metrics::MetricsManager::instance() != nullptr);
  {
    dariadb::utils::metrics::RAI_TimeMetric("group1", "metric2");
    {
      dariadb::utils::metrics::RAI_TimeMetric("group1", "metric1");
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    {
      dariadb::utils::metrics::RAI_TimeMetric("group1", "metric1");
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    using dariadb::utils::metrics::FloatMetric;
    ADD_METRICS("group2", "template",
                dariadb::utils::metrics::IMetric_Ptr{new FloatMetric(float(3.14))});
  }
  auto dump = dariadb::utils::metrics::MetricsManager::instance()->to_string();
  BOOST_CHECK(dump.size() > size_t(0));
}

BOOST_AUTO_TEST_CASE(ThreadsPool) {
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

  // while(1)
  {
    const size_t tasks_count = 10;
    ThreadManager::start(tpm_params);
	int called = 0;
	AsyncTask at_while = [&called](const ThreadInfo &ti) {
		if(called<10){
			++called;
			return true;
		}
		return false;
	};
    AsyncTask at1 = [tk1](const ThreadInfo &ti) {
      if (tk1 != ti.kind) {
        BOOST_TEST_MESSAGE("(tk1 != ti.kind)");
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
        throw MAKE_EXCEPTION("(tk1 != ti.kind)");
      }
	  return false;
    };
    AsyncTask at2 = [tk2](const ThreadInfo &ti) {
      if (tk2 != ti.kind) {
        BOOST_TEST_MESSAGE("(tk2 != ti.kind)");
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
        throw MAKE_EXCEPTION("(tk2 != ti.kind)");
      }
	  return false;
    };
	auto at_while_res=ThreadManager::instance()->post(tk1, AT(at_while));
    for (size_t i = 0; i < tasks_count; ++i) {
      //            logger("test #"<<i);
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
  std::string str = "1 2 3 4 5 6 7 8";
  auto splitted = dariadb::utils::strings::tokens(str);
  BOOST_CHECK_EQUAL(splitted.size(), size_t(8));

  splitted = dariadb::utils::strings::split(str, ' ');
  BOOST_CHECK_EQUAL(splitted.size(), size_t(8));
}
