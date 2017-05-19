
#include <libdariadb/utils/async/thread_manager.h>
#include <libdariadb/utils/async/thread_pool.h>
#include <libdariadb/utils/bitoperations.h>
#include <libdariadb/utils/crc.h>
#include <libdariadb/utils/cz.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/in_interval.h>
#include <libdariadb/utils/strings.h>
#include <libdariadb/utils/utils.h>

#include "helpers.h"
#include <catch.hpp>
#include <thread>

TEST_CASE("Utils.CountZero") {
  EXPECT_EQ(dariadb::utils::clz(67553994410557440), 8);
  EXPECT_EQ(dariadb::utils::clz(3458764513820540928), 2);
  EXPECT_EQ(dariadb::utils::clz(15), 60);

  EXPECT_EQ(dariadb::utils::ctz(240), 4);
  EXPECT_EQ(dariadb::utils::ctz(3840), 8);
}

TEST_CASE("Utils.InInterval") {
  EXPECT_TRUE(dariadb::utils::inInterval(1, 5, 1));
  EXPECT_TRUE(dariadb::utils::inInterval(1, 5, 2));
  EXPECT_TRUE(dariadb::utils::inInterval(1, 5, 5));
  EXPECT_TRUE(!dariadb::utils::inInterval(1, 5, 0));
  EXPECT_TRUE(!dariadb::utils::inInterval(0, 1, 2));
}

TEST_CASE("Utils.BitOperations") {
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

TEST_CASE("Utils.FileUtils") {
  std::string filename = "foo/bar/test.txt";
  EXPECT_EQ(dariadb::utils::fs::filename(filename), "test");
  EXPECT_EQ(dariadb::utils::fs::parent_path(filename), "foo/bar");
  EXPECT_EQ(dariadb::utils::fs::extract_filename(filename), "test.txt");

  auto ls_res = dariadb::utils::fs::ls(".");
  EXPECT_TRUE(ls_res.size() > 0);

  const std::string fname = "mapped_file.test";
  auto mapf = dariadb::utils::fs::MappedFile::touch(fname, 1024);
  for (uint8_t i = 0; i < 100; i++) {
    mapf->data()[i] = i;
  }
  mapf->close();

  ls_res = dariadb::utils::fs::ls(".", ".test");
  EXPECT_TRUE(ls_res.size() == 1);
  auto reopen_mapf = dariadb::utils::fs::MappedFile::open(fname);
  for (uint8_t i = 0; i < 100; i++) {
    EXPECT_EQ(reopen_mapf->data()[i], i);
  }
  reopen_mapf->close();
  dariadb::utils::fs::rm(fname);

  std::string parent_p = "path1";
  std::string child_p = "path2";
  auto concat_p = dariadb::utils::fs::append_path(parent_p, child_p);
  EXPECT_EQ(dariadb::utils::fs::parent_path(concat_p), parent_p);
}

TEST_CASE("Utils.SplitString") {
  std::string str = "1 2 3 4 5 6 7 8";
  auto splitted = dariadb::utils::strings::tokens(str);
  EXPECT_EQ(splitted.size(), size_t(8));

  splitted = dariadb::utils::strings::split(str, ' ');
  EXPECT_EQ(splitted.size(), size_t(8));
}

TEST_CASE("Utils.StringToUpper") {
  auto s = "lower string";
  auto res = dariadb::utils::strings::to_upper(s);
  EXPECT_EQ(res, "LOWER STRING");
}

TEST_CASE("Utils.StringToLower") {
  auto s = "UPPER STRING";
  auto res = dariadb::utils::strings::to_lower(s);
  EXPECT_EQ(res, "upper string");
}

TEST_CASE("Utils.ThreadsPool") {
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
        INFO("(tk != ti.kind)");
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
        INFO("(tk != ti.kind)");
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

TEST_CASE("Utils.ThreadsManager") {
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
        INFO("(tk != ti.kind)");
        dariadb::utils::sleep_mls(400);
        throw MAKE_EXCEPTION("(tk1 != ti.kind)");
      }
      return false;
    };
    AsyncTask at2 = [tk2](const ThreadInfo &ti) {
      if (tk2 != ti.kind) {
        INFO("(tk != ti.kind)");
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
    // EXPECT_GT(ThreadManager::instance()->active_works(), size_t(0));
    at_while_res->wait();
    EXPECT_EQ(called, int(10));
    ThreadManager::instance()->flush();
    ThreadManager::instance()->stop();
  }
}
