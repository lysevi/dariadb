#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <algorithm>
#include <boost/test/unit_test.hpp>

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

BOOST_AUTO_TEST_CASE(PageManagerReadWriteWithContinue) {
  const std::string storagePath = "testStorage";
  const size_t chunks_size = 200;
  auto t = dariadb::Time(0);

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  dariadb::MeasList addeded;

  auto settings = dariadb::storage::Settings::create(storagePath);
  settings->chunk_size.setValue(chunks_size);

  auto manifest = dariadb::storage::Manifest::create(settings);

  auto _engine_env = dariadb::storage::EngineEnvironment::create();
  _engine_env->addResource(
      dariadb::storage::EngineEnvironment::Resource::SETTINGS, settings.get());
  _engine_env->addResource(
      dariadb::storage::EngineEnvironment::Resource::MANIFEST, manifest.get());

  dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

  auto pm = dariadb::storage::PageManager::create(_engine_env);
  dariadb::Meas first;
  first.id = 1;
  first.time = t;
  {
    first.id = 1;
    first.time = t;
    auto count = chunks_size * 2;
    dariadb::MeasArray ma;
    ma.resize(count);
    for (size_t i = 0; i < count; i++, t++) {
      first.flag = dariadb::Flag(i);
      first.time = t;
      first.value = dariadb::Value(i);
      ma[i] = first;
      addeded.push_back(first);
    }
    pm->append("pagename", ma);
  }
  pm = nullptr;

  auto fname = dariadb::utils::fs::ls(settings->raw_path.value(),
                                      dariadb::storage::PAGE_FILE_EXT)
                   .front();
  auto header = dariadb::storage::Page::readFooter(fname);
  BOOST_CHECK(header.addeded_chunks != size_t(0));

  auto iheader = dariadb::storage::Page::readIndexFooter(fname + "i");
  BOOST_CHECK(iheader.stat.count != 0);

  pm = dariadb::storage::PageManager::create(_engine_env);

  auto mintime_chunks = pm->valuesBeforeTimePoint(
      dariadb::QueryTimePoint(dariadb::IdArray{1}, 0, pm->minTime()));
  BOOST_CHECK_GE(mintime_chunks.size(), size_t(1));
  pm->fsck();

  auto index_files = dariadb::utils::fs::ls(storagePath, ".pagei");
  for (auto &f : index_files) {
    dariadb::utils::fs::rm(f);
  }

  pm->fsck();

  mintime_chunks = pm->valuesBeforeTimePoint(
      dariadb::QueryTimePoint(dariadb::IdArray{1}, 0, pm->minTime()));
  BOOST_CHECK_GE(mintime_chunks.size(), size_t(1));

  pm = nullptr;
  manifest = nullptr;
  dariadb::utils::async::ThreadManager::stop();

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}

BOOST_AUTO_TEST_CASE(PageManagerMultiPageRead) {
  const std::string storagePath = "testStorage";
  const size_t chunks_size = 200;
  auto t = dariadb::Time(0);

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  dariadb::MeasList addeded;

  auto settings = dariadb::storage::Settings::create(storagePath);
  settings->chunk_size.setValue(chunks_size);

  auto manifest = dariadb::storage::Manifest::create(settings);

  auto _engine_env = dariadb::storage::EngineEnvironment::create();
  _engine_env->addResource(
      dariadb::storage::EngineEnvironment::Resource::SETTINGS, settings.get());
  _engine_env->addResource(
      dariadb::storage::EngineEnvironment::Resource::MANIFEST, manifest.get());

  dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

  auto pm = dariadb::storage::PageManager::create(_engine_env);
  dariadb::Meas first;
  first.id = 1;
  first.time = t;
  const size_t page_count = 4;

  size_t iteration = 0;
  while (dariadb::utils::fs::ls(settings->raw_path.value(),
                                dariadb::storage::PAGE_FILE_EXT)
             .size() <= page_count) {
    first.id = 1;
    first.time = t;
    auto count = chunks_size / 10;
    dariadb::MeasArray ma;
    ma.resize(count);
    for (size_t i = 0; i < count; i++, t++) {
      first.flag = dariadb::Flag(i);
      first.time = t;
      first.value = dariadb::Value(i);
      ma[i] = first;
      addeded.push_back(first);
    }
    std::stringstream ss;
    ss << std::string("pagename") << iteration++;
    pm->append(ss.str(), ma);
  }

  dariadb::QueryInterval qi(dariadb::IdArray{1}, 0,
                                     addeded.front().time, addeded.back().time);

  dariadb::QueryTimePoint qt(
      dariadb::IdArray{1}, 0,
      addeded.front().time + (addeded.back().time - addeded.front().time) / 2);

  // auto link_list = pm->linksByIterval(qi);

  auto clb = std::unique_ptr<dariadb::storage::MList_ReaderClb>{
      new dariadb::storage::MList_ReaderClb};
  pm->foreach (qi, clb.get());

  size_t writed = addeded.size();
  size_t readed = clb->mlist.size();

  BOOST_CHECK_EQUAL(readed, writed);

  auto id2meas = pm->valuesBeforeTimePoint(qt);

  BOOST_CHECK_EQUAL(id2meas.size(), qt.ids.size());
  for (auto kv : id2meas) {
    BOOST_CHECK_LE(kv.second.time, qt.time_point);
  }

  dariadb::Time minTime, maxTime;
  if (pm->minMaxTime(1, &minTime, &maxTime)) {
    BOOST_CHECK_EQUAL(minTime, qi.from);
    BOOST_CHECK_EQUAL(maxTime, qi.to);
  } else {
    BOOST_ERROR("PageManager::instance()->minMaxTime error!");
  }

  auto mm = pm->loadMinMax();
  BOOST_CHECK_EQUAL(mm.size(), size_t(1));

  auto page_before_erase =
      dariadb::utils::fs::ls(settings->raw_path.value(),
                             dariadb::storage::PAGE_FILE_EXT)
          .size();
  pm->eraseOld(addeded.back().time);
  auto page_after_erase =
      dariadb::utils::fs::ls(settings->raw_path.value(),
                             dariadb::storage::PAGE_FILE_EXT)
          .size();

  BOOST_CHECK_LT(page_after_erase, page_before_erase);
  BOOST_CHECK_EQUAL(page_after_erase, size_t(0));

  pm = nullptr;
  manifest = nullptr;
  dariadb::utils::async::ThreadManager::stop();

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}

BOOST_AUTO_TEST_CASE(PageManagerBulkWrite) {
  const std::string storagePath = "testStorage";
  const size_t chunks_size = 256;

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  auto settings = dariadb::storage::Settings::create(storagePath);
  settings->chunk_size.setValue(chunks_size);

  auto manifest = dariadb::storage::Manifest::create(settings);

  auto _engine_env = dariadb::storage::EngineEnvironment::create();
  _engine_env->addResource(
      dariadb::storage::EngineEnvironment::Resource::SETTINGS, settings.get());
  _engine_env->addResource(
      dariadb::storage::EngineEnvironment::Resource::MANIFEST, manifest.get());

  dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

  auto pm = dariadb::storage::PageManager::create(_engine_env);

  auto start_time = dariadb::Time(0);
  dariadb::MeasList addeded;
  const dariadb::Id id_count(5);
  dariadb::IdSet all_id_set;
  size_t count = 5000;
  dariadb::MeasArray a(count);
  auto e = dariadb::Meas();
  for (size_t i = 0; i < count; i++) {
    e.id = i % id_count;
    e.time++;
    e.value = dariadb::Value(i);
    a[i] = e;
    all_id_set.insert(e.id);
    addeded.push_back(e);
  }
  std::string page_file_prefix = "page_prefix";
  pm->append(page_file_prefix, a);

  dariadb::IdArray all_id_array{all_id_set.begin(), all_id_set.end()};
  { // Chunks load
    // must return all of appended chunks;

    pm->flush();
    dariadb::QueryInterval qi_all(all_id_array, 0, 0, e.time);
    // auto links_list = pm->linksByIterval(qi_all);

    auto clb1 = std::unique_ptr<dariadb::storage::MList_ReaderClb>{
        new dariadb::storage::MList_ReaderClb};

    pm->foreach (qi_all, clb1.get());

    BOOST_CHECK_EQUAL(addeded.size(), clb1->mlist.size());
    dariadb::Time minT = dariadb::MAX_TIME, maxT = dariadb::MIN_TIME;
    BOOST_CHECK(pm->minMaxTime(dariadb::Id(0), &minT, &maxT));
    BOOST_CHECK_EQUAL(minT, dariadb::Time(1));
    BOOST_CHECK_GE(maxT, minT);

    {
      dariadb::Time end_time(e.time / 2);
      dariadb::storage::ChunksList chunk_list;
      dariadb::QueryInterval qi(all_id_array, 0, start_time, end_time);
      // auto link_list = pm->linksByIterval(qi);

      auto clb2 = std::unique_ptr<dariadb::storage::MList_ReaderClb>{
          new dariadb::storage::MList_ReaderClb};

      pm->foreach (qi, clb2.get());

      BOOST_CHECK_GT(clb2->mlist.size(), size_t(0));

      for (auto &m : clb2->mlist) {
        BOOST_CHECK(m.time <= end_time);
      }

      auto id2meas_res = pm->valuesBeforeTimePoint(
          dariadb::QueryTimePoint(all_id_array, 0, end_time));
      BOOST_CHECK_EQUAL(id2meas_res.size(), all_id_array.size());

      for (auto &kv : id2meas_res) {
        BOOST_CHECK(kv.second.time <= end_time);
      }
    }
    pm = nullptr;
  }
  BOOST_CHECK(dariadb::utils::fs::path_exists(storagePath));
  using namespace dariadb::utils;
  BOOST_CHECK_EQUAL(fs::ls(fs::append_path(storagePath, "raw")).size(),
                    size_t(2)); // page +index
  manifest = nullptr;
  dariadb::utils::async::ThreadManager::stop();

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}

BOOST_AUTO_TEST_CASE(PageManagerRepack) {
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
  _engine_env->addResource(
      dariadb::storage::EngineEnvironment::Resource::SETTINGS, settings.get());
  _engine_env->addResource(
      dariadb::storage::EngineEnvironment::Resource::MANIFEST, manifest.get());

  dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

  auto pm = dariadb::storage::PageManager::create(_engine_env);

  size_t count = 100;
  dariadb::MeasArray a(count);
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
    a[i] = e;
  }
  std::string page_file_prefix1 = "page_prefix1";
  std::string page_file1 = page_file_prefix1 + ".page";
  pm->append(page_file_prefix1, a);

  e.time = 0;
  for (size_t i = 0; i < count; i++) {
    e.id = 1;
    e.time++;
    e.value = dariadb::Value(i);
    a[i] = e;
  }
  std::string page_file_prefix2 = "page_prefix2";
  std::string page_file2 = page_file_prefix2 + ".page";
  pm->append(page_file_prefix2, a);

  e.time = 0;
  for (size_t i = 0; i < count / 2; i++) {
    e.id = 0;
    e.time++;
    e.value = dariadb::Value(i) * 3;
    a[i] = e;
  }
  std::string page_file_prefix3 = "page_prefix3";
  std::string page_file3 = page_file_prefix3 + ".page";
  pm->append(page_file_prefix3, a);

  BOOST_CHECK(dariadb::utils::fs::path_exists(storagePath));
  BOOST_CHECK_EQUAL(
      dariadb::utils::fs::ls(settings->raw_path.value(), ".page").size(),
      size_t(3));

  { // id==0
    dariadb::QueryInterval qi({0}, 0, 0, dariadb::MAX_TIME);
    // auto link_list = pm->linksByIterval(qi);

    auto clb = std::unique_ptr<dariadb::storage::MList_ReaderClb>{
        new dariadb::storage::MList_ReaderClb};

    pm->foreach (qi, clb.get());

    BOOST_CHECK_GE(clb->mlist.size(), size_t(100));
  }

  auto pages_before = dariadb::utils::fs::ls(settings->raw_path.value(), ".page").size();
  pm->repack();
  auto pages_after = dariadb::utils::fs::ls(settings->raw_path.value(), ".page").size();
  BOOST_CHECK_LT(pages_after, pages_before);
  { // id==0
    dariadb::QueryInterval qi({0}, 0, 0, dariadb::MAX_TIME);
    // auto link_list = pm->linksByIterval(qi);

    auto clb = std::unique_ptr<dariadb::storage::MList_ReaderClb>{
        new dariadb::storage::MList_ReaderClb};

    pm->foreach (qi, clb.get());

    BOOST_CHECK_EQUAL(clb->mlist.size(), size_t(100));
  }
  { // id==1
    dariadb::QueryInterval qi({1}, 0, 0, dariadb::MAX_TIME);
    // auto link_list = pm->linksByIterval(qi);

    auto clb = std::unique_ptr<dariadb::storage::MList_ReaderClb>{
        new dariadb::storage::MList_ReaderClb};

    pm->foreach (qi, clb.get());

    BOOST_CHECK_EQUAL(clb->mlist.size(), size_t(100));
  }
  pm = nullptr;
  manifest = nullptr;
  dariadb::utils::async::ThreadManager::stop();

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}
