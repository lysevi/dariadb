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

#include <gtest/gtest.h>

#include <chrono>
#include <ctime>
#include <thread>

TEST(Utils, StrippedMapTest) {
  using namespace dariadb::utils;

  {
    stripped_map<int, uint64_t> default_ctor;
    EXPECT_EQ(default_ctor.size(), size_t(0));
  }

  {
    stripped_map<int, uint64_t> add;
    add.insert(int(1), uint64_t(2));
    add.insert(int(1), uint64_t(3));
    EXPECT_EQ(add.size(), size_t(1));
    uint64_t output = 0;
    EXPECT_TRUE(add.find(int(1), &output));
    EXPECT_EQ(output, uint64_t(3));
  }
  {

    stripped_map<int, uint64_t> add;
    {
      auto iter = add.find_bucket(1);
      iter.v->second = uint64_t(3);
    }
    uint64_t output = 0;
    EXPECT_TRUE(add.find(1, &output));
    EXPECT_EQ(output, uint64_t(3));
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
        EXPECT_EQ(output, uint64_t(for_search));
      } else {
        EXPECT_TRUE(false) << "key=" << for_search << " not found";
      }
    }
    size_t cnt = 0;
    auto f = [&cnt](const stripped_map<int, uint64_t>::value_type &kv) { cnt++; };
    add_many.apply(f);
    EXPECT_EQ(add_many.size(), size_t(key));
    EXPECT_EQ(add_many.size(), cnt);
  }
}

TEST(Utils, TimeToString) {
  auto ct = dariadb::timeutil::current_time();
  EXPECT_TRUE(ct != dariadb::Time(0));
  auto ct_str = dariadb::timeutil::to_string(ct);
  EXPECT_TRUE(ct_str.size() != 0);
}

TEST(Utils, TimeRound) {
  auto ct = dariadb::timeutil::current_time();
  {
    auto rounded = dariadb::timeutil::round_to_seconds(ct);
    auto rounded_d = dariadb::timeutil::to_datetime(rounded);
    EXPECT_EQ(rounded_d.millisecond, uint16_t(0));
  }

  {
    auto rounded = dariadb::timeutil::round_to_minutes(ct);
    auto rounded_d = dariadb::timeutil::to_datetime(rounded);
    EXPECT_EQ(rounded_d.millisecond, uint16_t(0));
    EXPECT_EQ(rounded_d.second, uint16_t(0));
  }

  {
    auto rounded = dariadb::timeutil::round_to_hours(ct);
    auto rounded_d = dariadb::timeutil::to_datetime(rounded);
    EXPECT_EQ(rounded_d.millisecond, uint16_t(0));
    EXPECT_EQ(rounded_d.second, uint16_t(0));
    EXPECT_EQ(rounded_d.minute, uint16_t(0));
  }
}

TEST(Utils, CountZero) {
  EXPECT_EQ(dariadb::utils::clz(67553994410557440), 8);
  EXPECT_EQ(dariadb::utils::clz(3458764513820540928), 2);
  EXPECT_EQ(dariadb::utils::clz(15), 60);

  EXPECT_EQ(dariadb::utils::ctz(240), 4);
  EXPECT_EQ(dariadb::utils::ctz(3840), 8);
}

TEST(Utils, InInterval) {
  EXPECT_TRUE(dariadb::utils::inInterval(1, 5, 1));
  EXPECT_TRUE(dariadb::utils::inInterval(1, 5, 2));
  EXPECT_TRUE(dariadb::utils::inInterval(1, 5, 5));
  EXPECT_TRUE(!dariadb::utils::inInterval(1, 5, 0));
  EXPECT_TRUE(!dariadb::utils::inInterval(0, 1, 2));
}

TEST(Utils, BitOperations) {
  uint8_t value = 0;
  for (int8_t i = 0; i < 7; i++) {
    value = dariadb::utils::BitOperations::set(value, i);
    EXPECT_EQ(dariadb::utils::BitOperations::check(value, i), true);
  }

  for (int8_t i = 0; i < 7; i++) {
    value = dariadb::utils::BitOperations::clr(value, i);
  }

  for (int8_t i = 0; i < 7; i++) {
    EXPECT_EQ(dariadb::utils::BitOperations::check(value, i), false);
  }
}

TEST(Utils, FileUtils) {
  std::string filename = "foo/bar/test.txt";
  EXPECT_EQ(dariadb::utils::fs::filename(filename), "test");
  EXPECT_EQ(dariadb::utils::fs::parent_path(filename), "foo/bar");
  EXPECT_EQ(dariadb::utils::fs::extract_filename(filename), "test.txt");

  auto ls_res = dariadb::utils::fs::ls(".");
  EXPECT_TRUE(ls_res.size() > 0);

  std::string parent_p = "path1";
  std::string child_p = "path2";
  auto concat_p = dariadb::utils::fs::append_path(parent_p, child_p);
  EXPECT_EQ(dariadb::utils::fs::parent_path(concat_p), parent_p);
}

TEST(Utils, SplitString) {
	std::string str = "1 2 3 4 5 6 7 8";
	auto splitted = dariadb::utils::strings::tokens(str);
	EXPECT_EQ(splitted.size(), size_t(8));

	splitted = dariadb::utils::strings::split(str, ' ');
	EXPECT_EQ(splitted.size(), size_t(8));
}

TEST(Utils, ThreadsPool) {
  using namespace dariadb::utils::async;

  const ThreadKind tk = 1;
  {
    const size_t threads_count = 2;
    ThreadPool tp(ThreadPool::Params(threads_count, tk));

    EXPECT_EQ(tp.threads_count(), threads_count);
    EXPECT_TRUE(!tp.isStoped());
    tp.stop();
    EXPECT_TRUE(tp.isStoped());
  }

  {
    const size_t threads_count = 2;
    ThreadPool tp(ThreadPool::Params(threads_count, tk));
    const size_t tasks_count = 100;
    AsyncTask at = [tk](const ThreadInfo &ti) {
      if (tk != ti.kind) {
        EXPECT_TRUE(false) << "(tk != ti.kind)";
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
        EXPECT_TRUE(false) << "(tk != ti.kind)";
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

TEST(Utils, ThreadsManager) {
  using namespace dariadb::utils::async;

  const ThreadKind tk1 = 1;
  const ThreadKind tk2 = 2;
  size_t threads_count = 2;
  ThreadPool::Params tp1(threads_count, tk1);
  ThreadPool::Params tp2(threads_count, tk2);

  ThreadManager::Params tpm_params(std::vector<ThreadPool::Params>{tp1, tp2});
  {
    EXPECT_TRUE(ThreadManager::instance() == nullptr);
    ThreadManager::start(tpm_params);
    EXPECT_TRUE(ThreadManager::instance() != nullptr);
    ThreadManager::stop();
    EXPECT_TRUE(ThreadManager::instance() == nullptr);
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
        EXPECT_TRUE(false) << "(tk != ti.kind)";
        dariadb::utils::sleep_mls(400);
        throw MAKE_EXCEPTION("(tk1 != ti.kind)");
      }
      return false;
    };
    AsyncTask at2 = [tk2](const ThreadInfo &ti) {
      if (tk2 != ti.kind) {
        EXPECT_TRUE(false) << "(tk != ti.kind)";
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
    EXPECT_GE(ThreadManager::instance()->active_works(), size_t(0));
    at_while_res->wait();
    EXPECT_EQ(called, int(10));
    ThreadManager::instance()->flush();
    ThreadManager::instance()->stop();
  }
}

