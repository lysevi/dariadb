#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <atomic>
#include <boost/test/unit_test.hpp>
#include <cassert>
#include <map>
#include <thread>

#include "test_common.h"
#include <storage/aof_manager.h>
#include <storage/aofile.h>
#include <storage/manifest.h>
#include <timeutil.h>
#include <utils/fs.h>
#include <utils/thread_manager.h>

class Moc_Dropper : public dariadb::storage::AofFileDropper {
  std::string _storage_path;

public:
  size_t writed_count;
  std::set<std::string> files;
  Moc_Dropper(std::string storage_path) {
    writed_count = 0;
    _storage_path = storage_path;
  }
  void drop(const dariadb::storage::AOFile_Ptr aof, const std::string fname) override {
    auto ma = aof->readAll();
    writed_count += ma.size();
    files.insert(fname);
    dariadb::storage::Manifest::instance()->aof_rm(fname);
    dariadb::utils::fs::rm(dariadb::utils::fs::append_path(_storage_path, fname));
  }
};

BOOST_AUTO_TEST_CASE(AofInitTest) {
  const size_t block_size = 1000;
  auto storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  dariadb::utils::fs::mkdir(storage_path);
  dariadb::storage::Manifest::start(
      dariadb::utils::fs::append_path(storage_path, "Manifest"));
  auto aof_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::AOF_FILE_EXT);
  assert(aof_files.size() == 0);
  auto p = dariadb::storage::AOFile::Params(block_size, storage_path);
  size_t writes_count = block_size;

  dariadb::IdSet id_set;
  {
    dariadb::storage::AOFile aof{p};

    aof_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::AOF_FILE_EXT);
    BOOST_CHECK_EQUAL(aof_files.size(), size_t(0));

    auto e = dariadb::Meas::empty();

    size_t id_count = 10;

    size_t i = 0;
    e.id = i % id_count;
    id_set.insert(e.id);
    e.time = dariadb::Time(i);
    e.value = dariadb::Value(i);
    BOOST_CHECK(aof.append(e).writed == 1);
    i++;
    dariadb::Meas::MeasList ml;
    for (; i < writes_count / 2; i++) {
      e.id = i % id_count;
      id_set.insert(e.id);
      e.time = dariadb::Time(i);
      e.value = dariadb::Value(i);
      ml.push_back(e);
    }
    aof.append(ml.begin(),ml.end());

    dariadb::Meas::MeasArray ma;
    ma.resize(writes_count - i);
    size_t pos = 0;
    for (; i < writes_count; i++) {
      e.id = i % id_count;
      id_set.insert(e.id);
      e.time = dariadb::Time(i);
      e.value = dariadb::Value(i);
      ma[pos] = e;
      pos++;
    }
    aof.append(ma.begin(),ma.end());
    aof_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::AOF_FILE_EXT);
    BOOST_CHECK_EQUAL(aof_files.size(), size_t(1));

    dariadb::Meas::MeasList out;

    out = aof.readInterval(dariadb::storage::QueryInterval(
        dariadb::IdArray(id_set.begin(), id_set.end()), 0, 0, writes_count));
    BOOST_CHECK_EQUAL(out.size(), writes_count);
  }
  {
    auto aof_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::AOF_FILE_EXT);
    BOOST_CHECK(aof_files.size() == size_t(1));
    dariadb::storage::AOFile aof(p, aof_files.front(), true);
    auto all = aof.readAll();
    BOOST_CHECK_EQUAL(all.size(), writes_count);
  }
  dariadb::storage::Manifest::stop();
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(AOFileCommonTest) {
  const size_t block_size = 10000;
  auto storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  {
    dariadb::utils::fs::mkdir(storage_path);
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storage_path, "Manifest"));
    auto aof_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::AOF_FILE_EXT);
    BOOST_CHECK(aof_files.size() == size_t(0));
    auto p = dariadb::storage::AOFile::Params(block_size, storage_path);
    dariadb::storage::AOFile aof(p);

    dariadb_test::storage_test_check(&aof, 0, 100, 1);
    dariadb::storage::Manifest::stop();
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(AOFManager_Instance) {
  const std::string storagePath = "testStorage";
  const size_t max_size = 10;
  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  dariadb::utils::fs::mkdir(storagePath);

  dariadb::utils::async::ThreadManager::start(
      dariadb::utils::async::THREAD_MANAGER_COMMON_PARAMS);
  dariadb::storage::Manifest::start(
      dariadb::utils::fs::append_path(storagePath, "Manifest"));

  dariadb::storage::AOFManager::start(
      dariadb::storage::AOFManager::Params(storagePath, max_size));

  BOOST_CHECK(dariadb::storage::AOFManager::instance() != nullptr);

  auto aof_files = dariadb::utils::fs::ls(storagePath, dariadb::storage::AOF_FILE_EXT);
  BOOST_CHECK_EQUAL(aof_files.size(), size_t(0));

  dariadb::storage::AOFManager::stop();
  dariadb::storage::Manifest::stop();
  dariadb::utils::async::ThreadManager::stop();

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}

BOOST_AUTO_TEST_CASE(AofManager_CommonTest) {
  const std::string storagePath = "testStorage";
  const size_t max_size = 150;
  const dariadb::Time from = 0;
  const dariadb::Time to = from + 1021;
  const dariadb::Time step = 10;

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  dariadb::utils::fs::mkdir(storagePath);
  {
    dariadb::utils::async::ThreadManager::start(
        dariadb::utils::async::THREAD_MANAGER_COMMON_PARAMS);
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storagePath, "Manifest"));
    dariadb::storage::AOFManager::start(
        dariadb::storage::AOFManager::Params(storagePath, max_size));

    dariadb_test::storage_test_check(dariadb::storage::AOFManager::instance(), from, to,
                                     step);

    dariadb::storage::AOFManager::stop();
    dariadb::storage::Manifest::stop();
    dariadb::utils::async::ThreadManager::stop();
  }
  {
    std::shared_ptr<Moc_Dropper> stor(new Moc_Dropper(storagePath));
    stor->writed_count = 0;
    dariadb::utils::async::ThreadManager::start(
        dariadb::utils::async::THREAD_MANAGER_COMMON_PARAMS);
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storagePath, "Manifest"));
    dariadb::storage::AOFManager::start(
        dariadb::storage::AOFManager::Params(storagePath, max_size));

    dariadb::storage::QueryInterval qi(dariadb::IdArray{0}, dariadb::Flag(), from, to);
    auto out= dariadb::storage::AOFManager::instance()->readInterval(qi);
    BOOST_CHECK_EQUAL(out.size(), dariadb_test::copies_count);

    auto closed = dariadb::storage::AOFManager::instance()->closed_aofs();
    BOOST_CHECK(closed.size() != size_t(0));

    for (auto fname : closed) {
      dariadb::storage::AOFManager::instance()->drop_aof(fname, stor.get());
    }

    BOOST_CHECK(stor->writed_count != size_t(0));
    BOOST_CHECK_EQUAL(stor->files.size(), closed.size());

    closed = dariadb::storage::AOFManager::instance()->closed_aofs();
    BOOST_CHECK_EQUAL(closed.size(), size_t(0));

    dariadb::storage::AOFManager::stop();
    dariadb::storage::Manifest::stop();
    dariadb::utils::async::ThreadManager::stop();
  }
  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}
