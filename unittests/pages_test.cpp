#include <gtest/gtest.h>

#include "helpers.h"

#include <algorithm>
#include <iostream>

#include <libdariadb/flags.h>
#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/callbacks.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/storage/pages/page.h>
#include <libdariadb/storage/pages/page_manager.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/utils.h>

using dariadb::storage::PageManager;
using dariadb::storage::Manifest;

TEST(PageManager, ReadWriteWithContinue) {
  const std::string storagePath = "testStorage";
  const size_t chunks_size = 200;
  auto t = dariadb::Time(0);

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  dariadb_test::MeasesList addeded;

  auto settings = dariadb::storage::Settings::create(storagePath);
  settings->chunk_size.setValue(chunks_size);

  auto manifest = dariadb::storage::Manifest::create(settings);

  auto _engine_env = dariadb::storage::EngineEnvironment::create();
  _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
                           settings.get());
  _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::MANIFEST,
                           manifest.get());

  dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

  auto pm = dariadb::storage::PageManager::create(_engine_env);
  dariadb::Meas first;
  first.id = 1;
  first.time = t;
  {
    first.id = 1;
    first.time = t;
    auto count = chunks_size * 2;
    dariadb::SplitedById ma;
    for (size_t i = 0; i < count; i++, t++) {
      first.flag = dariadb::Flag(i);
      first.time = t;
      first.value = dariadb::Value(i);
      ma[first.id].push_back(first);
      addeded.push_back(first);
    }
    bool complete = false;
    pm->append_async("pagename", ma, [&complete](auto p) { complete = true; });
    while (!complete) {
      dariadb::utils::sleep_mls(100);
    }
  }
  pm = nullptr;

  auto fname =
      dariadb::utils::fs::ls(settings->raw_path.value(), dariadb::storage::PAGE_FILE_EXT)
          .front();
  auto header = dariadb::storage::Page::readFooter(fname);
  EXPECT_TRUE(header.addeded_chunks != size_t(0));

  auto iheader = dariadb::storage::Page::readIndexFooter(fname + "i");
  EXPECT_TRUE(iheader.stat.count != 0);

  pm = dariadb::storage::PageManager::create(_engine_env);

  auto mintime_chunks = pm->valuesBeforeTimePoint(
      dariadb::QueryTimePoint(dariadb::IdArray{1}, 0, pm->minTime()));
  EXPECT_GE(mintime_chunks.size(), size_t(1));
  pm->fsck();

  auto index_files = dariadb::utils::fs::ls(storagePath, ".pagei");
  for (auto &f : index_files) {
    dariadb::utils::fs::rm(f);
  }

  pm->fsck();

  mintime_chunks = pm->valuesBeforeTimePoint(
      dariadb::QueryTimePoint(dariadb::IdArray{1}, 0, pm->minTime()));
  EXPECT_GE(mintime_chunks.size(), size_t(1));

  pm = nullptr;
  manifest = nullptr;
  dariadb::utils::async::ThreadManager::stop();

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}

TEST(PageManager, MultiPageRead) {
  const std::string storagePath = "testStorage";
  const size_t chunks_size = 200;
  auto t = dariadb::Time(0);

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  dariadb_test::MeasesList addeded;

  auto settings = dariadb::storage::Settings::create(storagePath);
  settings->chunk_size.setValue(chunks_size);

  auto manifest = dariadb::storage::Manifest::create(settings);

  auto _engine_env = dariadb::storage::EngineEnvironment::create();
  _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
                           settings.get());
  _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::MANIFEST,
                           manifest.get());

  dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

  auto pm = dariadb::storage::PageManager::create(_engine_env);
  dariadb::Meas first;
  first.id = 1;
  first.time = t;
  const size_t page_count = 4;

  size_t iteration = 0;
  while (
      dariadb::utils::fs::ls(settings->raw_path.value(), dariadb::storage::PAGE_FILE_EXT)
          .size() <= page_count) {
    first.id = 1;
    first.time = t;
    auto count = chunks_size / 10;
    dariadb::SplitedById ma;
    for (size_t i = 0; i < count; i++, t++) {
      first.flag = dariadb::Flag(i);
      first.time = t;
      first.value = dariadb::Value(i);
      ma[first.id].push_back(first);
      addeded.push_back(first);
    }
    std::stringstream ss;
    ss << std::string("pagename") << iteration++;
    bool complete = false;
    pm->append_async(ss.str(), ma, [&complete](auto p) { complete = true; });
    while (!complete) {
      dariadb::utils::sleep_mls(100);
    }
  }

  dariadb::QueryInterval qi(dariadb::IdArray{1}, 0, addeded.front().time,
                            addeded.back().time);

  dariadb::QueryTimePoint qt(dariadb::IdArray{1}, 0,
                             addeded.front().time +
                                 (addeded.back().time - addeded.front().time) / 2);

  // auto link_list = pm->linksByIterval(qi);

  auto clb = std::unique_ptr<dariadb::storage::MArray_ReaderClb>{
      new dariadb::storage::MArray_ReaderClb(addeded.size())};
  pm->foreach (qi, clb.get());

  size_t writed = addeded.size();
  size_t readed = clb->marray.size();

  EXPECT_EQ(readed, writed);

  auto id2meas = pm->valuesBeforeTimePoint(qt);

  EXPECT_EQ(id2meas.size(), qt.ids.size());
  for (auto kv : id2meas) {
    EXPECT_LE(kv.second.time, qt.time_point);
  }

  dariadb::Time minTime, maxTime;
  if (pm->minMaxTime(1, &minTime, &maxTime)) {
    EXPECT_EQ(minTime, qi.from);
    EXPECT_EQ(maxTime, qi.to);
  } else {
    EXPECT_TRUE(false) << "PageManager::instance()->minMaxTime error!";
  }

  auto mm = pm->loadMinMax();
  EXPECT_EQ(mm->size(), size_t(1));

  auto page_before_erase =
      dariadb::utils::fs::ls(settings->raw_path.value(), dariadb::storage::PAGE_FILE_EXT)
          .size();
  pm->eraseOld(addeded.back().id, addeded.back().time);
  auto page_after_erase =
      dariadb::utils::fs::ls(settings->raw_path.value(), dariadb::storage::PAGE_FILE_EXT)
          .size();

  EXPECT_LT(page_after_erase, page_before_erase);
  EXPECT_EQ(page_after_erase, size_t(0));

  pm = nullptr;
  manifest = nullptr;
  dariadb::utils::async::ThreadManager::stop();

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}

TEST(PageManager, BulkWrite) {
  const std::string storagePath = "testStorage";
  const size_t chunks_size = 256;

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  auto settings = dariadb::storage::Settings::create(storagePath);
  settings->chunk_size.setValue(chunks_size);

  auto manifest = dariadb::storage::Manifest::create(settings);

  auto _engine_env = dariadb::storage::EngineEnvironment::create();
  _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
                           settings.get());
  _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::MANIFEST,
                           manifest.get());

  dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

  auto pm = dariadb::storage::PageManager::create(_engine_env);

  auto start_time = dariadb::Time(0);
  dariadb_test::MeasesList addeded;
  const dariadb::Id id_count(1);
  dariadb::IdSet all_id_set;
  size_t count = 5000;
  dariadb::SplitedById a;
  auto e = dariadb::Meas();
  for (size_t i = 0; i < count; i++) {
    e.id = i % id_count;
    e.time++;
    e.value = dariadb::Value(i);
    a[e.id].push_back(e);
    all_id_set.insert(e.id);
    addeded.push_back(e);
  }
  std::string page_file_prefix = "page_prefix";
  bool complete = false;
  pm->append_async(page_file_prefix, a, [&complete](auto p) { complete = true; });
  while (!complete) {
    dariadb::utils::sleep_mls(100);
  }

  dariadb::IdArray all_id_array{all_id_set.begin(), all_id_set.end()};
  { // Chunks load
    // must return all of appended chunks;

    pm->flush();
    dariadb::QueryInterval qi_all(all_id_array, 0, 0, e.time);
    // auto links_list = pm->linksByIterval(qi_all);

    auto clb1 = std::unique_ptr<dariadb::storage::MArray_ReaderClb>{
        new dariadb::storage::MArray_ReaderClb(addeded.size())};

    pm->foreach (qi_all, clb1.get());

    EXPECT_EQ(addeded.size(), clb1->marray.size());
    dariadb::Time minT = dariadb::MAX_TIME, maxT = dariadb::MIN_TIME;
    EXPECT_TRUE(pm->minMaxTime(dariadb::Id(0), &minT, &maxT));
    EXPECT_EQ(minT, dariadb::Time(1));
    EXPECT_GE(maxT, minT);

    {
      dariadb::Time end_time(e.time / 2);
      dariadb::storage::ChunksList chunk_list;
      dariadb::QueryInterval qi(all_id_array, 0, start_time, end_time);
      // auto link_list = pm->linksByIterval(qi);

      auto clb2 = std::unique_ptr<dariadb::storage::MArray_ReaderClb>{
          new dariadb::storage::MArray_ReaderClb(0)};

      pm->foreach (qi, clb2.get());

      EXPECT_GT(clb2->marray.size(), size_t(0));

      for (auto &m : clb2->marray) {
        EXPECT_TRUE(m.time <= end_time);
      }

      auto id2meas_res =
          pm->valuesBeforeTimePoint(dariadb::QueryTimePoint(all_id_array, 0, end_time));
      EXPECT_EQ(id2meas_res.size(), all_id_array.size());

      for (auto &kv : id2meas_res) {
        EXPECT_TRUE(kv.second.time <= end_time);
      }
    }
    pm = nullptr;
  }
  EXPECT_TRUE(dariadb::utils::fs::path_exists(storagePath));
  using namespace dariadb::utils;
  EXPECT_EQ(fs::ls(fs::append_path(storagePath, "raw")).size(), size_t(2)); // page +index
  manifest = nullptr;
  dariadb::utils::async::ThreadManager::stop();

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}

TEST(PageManager, Repack) {
  const std::string storagePath = "testStorage";
  const size_t chunks_size = 256;

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  auto settings = dariadb::storage::Settings::create(storagePath);
  settings->chunk_size.setValue(chunks_size);
  settings->max_pages_in_level.setValue(2);
  auto manifest = dariadb::storage::Manifest::create(settings);

  auto _engine_env = dariadb::storage::EngineEnvironment::create();
  _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
                           settings.get());
  _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::MANIFEST,
                           manifest.get());

  dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

  auto pm = dariadb::storage::PageManager::create(_engine_env);

  size_t count = 100;
  dariadb::SplitedById a;
  /**
  Id=0 =>  0, count
  Id=1 =>  0, count
  Id=0 =>  0, count/2
  Id=1 =>  count/2, count
  */
  auto e = dariadb::Meas();
  for (size_t i = 0; i < count; i++) {
    e.id = 0;
    e.time++;
    e.value = dariadb::Value(i);
    a[e.id].push_back(e);
  }
  std::string page_file_prefix1 = "page_prefix1";
  std::string page_file1 = page_file_prefix1 + ".page";
  bool complete = false;
  pm->append_async(page_file_prefix1, a, [&complete](auto p) { complete = true; });
  while (!complete) {
    dariadb::utils::sleep_mls(100);
  }

  a.clear();
  e.time = 0;
  for (size_t i = 0; i < count; i++) {
    e.id = 1;
    e.time++;
    e.value = dariadb::Value(i);
    a[e.id].push_back(e);
  }
  std::string page_file_prefix2 = "page_prefix2";
  std::string page_file2 = page_file_prefix2 + ".page";
  complete = false;
  pm->append_async(page_file_prefix2, a, [&complete](auto p) { complete = true; });
  while (!complete) {
    dariadb::utils::sleep_mls(100);
  }

  a.clear();
  e.time = 0;
  for (size_t i = 0; i < count / 2; i++) {
    e.id = 0;
    e.time++;
    e.value = dariadb::Value(i) * 3;
    a[e.id].push_back(e);
  }
  std::string page_file_prefix3 = "page_prefix3";
  std::string page_file3 = page_file_prefix3 + ".page";
  complete = false;
  pm->append_async(page_file_prefix3, a, [&complete](auto p) { complete = true; });
  while (!complete) {
    dariadb::utils::sleep_mls(100);
  }

  EXPECT_TRUE(dariadb::utils::fs::path_exists(storagePath));
  EXPECT_EQ(dariadb::utils::fs::ls(settings->raw_path.value(), ".page").size(),
            size_t(3));

  { // id==0
    dariadb::QueryInterval qi({0}, 0, 0, dariadb::MAX_TIME);
    // auto link_list = pm->linksByIterval(qi);

    auto clb = std::unique_ptr<dariadb::storage::MArray_ReaderClb>{
        new dariadb::storage::MArray_ReaderClb(count)};

    pm->foreach (qi, clb.get());

    EXPECT_GE(clb->marray.size(), count);
  }

  auto pages_before = dariadb::utils::fs::ls(settings->raw_path.value(), ".page");
  pm->repack(dariadb::Id(0));
  auto pages_after = dariadb::utils::fs::ls(settings->raw_path.value(), ".page");
  EXPECT_LT(pages_after.size(), pages_before.size());
  { // id==0
    dariadb::QueryInterval qi({0}, 0, 0, dariadb::MAX_TIME);

    auto clb = std::unique_ptr<dariadb::storage::MArray_ReaderClb>{
        new dariadb::storage::MArray_ReaderClb(count)};

    pm->foreach (qi, clb.get());

    EXPECT_EQ(clb->marray.size(), count);
  }
  { // id==1
    dariadb::QueryInterval qi({1}, 0, 0, dariadb::MAX_TIME);

    auto clb = std::unique_ptr<dariadb::storage::MArray_ReaderClb>{
        new dariadb::storage::MArray_ReaderClb(count)};

    pm->foreach (qi, clb.get());

    EXPECT_EQ(clb->marray.size(), count);
  }
  pm = nullptr;
  manifest = nullptr;
  dariadb::utils::async::ThreadManager::stop();

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}

class RmAllCompaction : public dariadb::ICompactionController {
public:
  RmAllCompaction(dariadb::Id id, dariadb::Time from, dariadb::Time to)
      : dariadb::ICompactionController(id, dariadb::MAX_TIME, from, to) {}

  void compact(dariadb::MeasArray &values, std::vector<int> &filter) override {
    EXPECT_TRUE(values.size() == filter.size());

    for (size_t i = 0; i < values.size(); ++i) {
      filter[i] = int(0);
    }
  }
};

class TestCompaction : public dariadb::ICompactionController {
public:
  TestCompaction(dariadb::Id id, dariadb::Time from, dariadb::Time to)
      : dariadb::ICompactionController(id, dariadb::MAX_TIME, from, to) {
    calls = 0;
  }

  void compact(dariadb::MeasArray &values, std::vector<int> &filter) override {
    EXPECT_TRUE(values.size() == filter.size());
    calls++;
    for (size_t i = 0; i < values.size(); ++i) {
      auto v = values[i];
      EXPECT_TRUE(filter[i] == int(1));
      if (v.id == dariadb::Id(0)) {
        values[i].flag = 777;
      } else {
        filter[i] = v.time % 2 ? int(1) : int(0);
      }
    }
  }

  size_t calls;
};

TEST(PageManager, Compact) {
  const std::string storagePath = "testStorage";
  const size_t chunks_size = 256;

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  auto settings = dariadb::storage::Settings::create(storagePath);
  settings->chunk_size.setValue(chunks_size);
  settings->max_pages_in_level.setValue(2);
  auto manifest = dariadb::storage::Manifest::create(settings);

  auto _engine_env = dariadb::storage::EngineEnvironment::create();
  _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
                           settings.get());
  _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::MANIFEST,
                           manifest.get());

  dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

  auto pm = dariadb::storage::PageManager::create(_engine_env);

  size_t count = 100;
  dariadb::SplitedById a;
  /**
  Id=0 =>  0, count
  Id=1 =>  0, count
  Id=0 =>  0, count/2
  Id=1 =>  count/2, count
  */
  auto e = dariadb::Meas();
  for (size_t i = 0; i < count; i++) {
    e.id = 0;
    e.time++;
    e.value = dariadb::Value(i);
    a[e.id].push_back(e);
  }
  std::string page_file_prefix1 = "page_prefix1";
  std::string page_file1 = page_file_prefix1 + ".page";
  bool complete = false;
  pm->append_async(page_file_prefix1, a, [&complete](auto p) { complete = true; });
  while (!complete) {
    dariadb::utils::sleep_mls(100);
  }
  a.clear();
  e.time = 0;
  for (size_t i = 0; i < count; i++) {
    e.id = 1;
    e.time++;
    e.value = dariadb::Value(i);
    a[e.id].push_back(e);
  }
  std::string page_file_prefix2 = "page_prefix2";
  std::string page_file2 = page_file_prefix2 + ".page";

  complete = false;
  pm->append_async(page_file_prefix2, a, [&complete](auto p) { complete = true; });
  while (!complete) {
    dariadb::utils::sleep_mls(100);
  }
  a.clear();
  e.time = 0;
  for (size_t i = 0; i < count / 2; i++) {
    e.id = 0;
    e.time++;
    e.value = dariadb::Value(i) * 3;
    a[e.id].push_back(e);
  }
  std::string page_file_prefix3 = "page_prefix3";
  std::string page_file3 = page_file_prefix3 + ".page";

  complete = false;
  pm->append_async(page_file_prefix3, a, [&complete](auto p) { complete = true; });
  while (!complete) {
    dariadb::utils::sleep_mls(100);
  }

  EXPECT_TRUE(dariadb::utils::fs::path_exists(storagePath));
  EXPECT_EQ(dariadb::utils::fs::ls(settings->raw_path.value(), ".page").size(),
            size_t(3));

  { // id==0
    dariadb::QueryInterval qi({0}, 0, 0, dariadb::MAX_TIME);
    // auto link_list = pm->linksByIterval(qi);

    auto clb = std::unique_ptr<dariadb::storage::MArray_ReaderClb>{
        new dariadb::storage::MArray_ReaderClb(count)};

    pm->foreach (qi, clb.get());

    EXPECT_GE(clb->marray.size(), count);
  }
  {
    auto compaction_logic_0 = std::make_unique<TestCompaction>(
        dariadb::Id(0), dariadb::Time(0), dariadb::Time(count / 2));

    auto compaction_logic_1 = std::make_unique<TestCompaction>(
        dariadb::Id(1), dariadb::Time(1), dariadb::Time(count / 2));

    auto pages_before =
        dariadb::utils::fs::ls(settings->raw_path.value(), ".page").size();
    pm->compact(compaction_logic_0.get());
    pm->compact(compaction_logic_1.get());

    EXPECT_TRUE(compaction_logic_0->calls == size_t(1));
    EXPECT_TRUE(compaction_logic_1->calls == size_t(1));

    auto pages_after = dariadb::utils::fs::ls(settings->raw_path.value(), ".page").size();
    EXPECT_LT(pages_after, pages_before);
  }
  { // id==0
    dariadb::QueryInterval qi({0}, 0, 0, dariadb::MAX_TIME);

    auto clb = std::make_unique<dariadb::storage::MArray_ReaderClb>(count);

    pm->foreach (qi, clb.get());

    EXPECT_EQ(clb->marray.size(), count);
    for (auto v : clb->marray) {
      EXPECT_EQ(v.flag, dariadb::Flag(777));
    }
  }
  { // id==1
    dariadb::QueryInterval qi({1}, 0, 0, dariadb::MAX_TIME);

    auto clb = std::make_unique<dariadb::storage::MArray_ReaderClb>(count);

    pm->foreach (qi, clb.get());

    EXPECT_EQ(clb->marray.size(), count / 2);
    for (auto v : clb->marray) {
      EXPECT_TRUE(v.time % 2);
    }
  }
  {
    auto compaction_logic_0 = std::make_unique<RmAllCompaction>(
        dariadb::Id(0), dariadb::Time(0), dariadb::Time(count / 2));
    auto compaction_logic_1 = std::make_unique<RmAllCompaction>(
        dariadb::Id(1), dariadb::Time(0), dariadb::Time(count / 2));

    pm->compact(compaction_logic_0.get());
    pm->compact(compaction_logic_1.get());

    auto pages_after = dariadb::utils::fs::ls(settings->raw_path.value(), ".page");
    EXPECT_TRUE(pages_after.empty());

    auto index_after = dariadb::utils::fs::ls(settings->raw_path.value(), ".pagei");
    EXPECT_TRUE(index_after.empty());
  }
  pm = nullptr;
  manifest = nullptr;
  dariadb::utils::async::ThreadManager::stop();

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}
