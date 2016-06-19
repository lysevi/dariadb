#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <atomic>
#include <boost/test/unit_test.hpp>
#include <cassert>
#include <map>
#include <thread>

#include "test_common.h"
#include <storage/aofile.h>
#include <storage/aof_manager.h>
#include <storage/manifest.h>
#include <timeutil.h>
#include <utils/fs.h>

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

BOOST_AUTO_TEST_CASE(AofInitTest) {
  const size_t block_size = 1000;
  auto storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  dariadb::utils::fs::mkdir(storage_path);
  auto aof_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::AOF_FILE_EXT);
  assert(aof_files.size() == 0);
  auto p = dariadb::storage::AOFile::Params(block_size, dariadb::utils::fs::append_path(storage_path, "test.aof"));
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
    for (; i < writes_count/2; i++) {
      e.id = i % id_count;
      id_set.insert(e.id);
      e.time = dariadb::Time(i);
      e.value = dariadb::Value(i);
      ml.push_back(e);
    }
    aof.append(ml);

    dariadb::Meas::MeasArray ma;
    ma.resize(writes_count-i);
    size_t pos=0;
    for (; i < writes_count; i++) {
      e.id = i % id_count;
      id_set.insert(e.id);
      e.time = dariadb::Time(i);
      e.value = dariadb::Value(i);
      ma[pos]=e;
      pos++;
    }
    aof.append(ma);
    aof_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::AOF_FILE_EXT);
    BOOST_CHECK_EQUAL(aof_files.size(), size_t(1));

    dariadb::Meas::MeasList out;

    auto reader = aof.readInterval(dariadb::storage::QueryInterval(
        dariadb::IdArray(id_set.begin(), id_set.end()), 0, 0, writes_count));
    BOOST_CHECK(reader != nullptr);
    reader->readAll(&out);
    BOOST_CHECK_EQUAL(out.size(), writes_count);
  }

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
  {
    dariadb::utils::fs::mkdir(storage_path);
    auto aof_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::AOF_FILE_EXT);
    assert(aof_files.size() == 0);
    auto p = dariadb::storage::AOFile::Params(block_size, dariadb::utils::fs::append_path(storage_path, "test.aof"));
    dariadb::storage::AOFile aof(p);

    dariadb_test::storage_test_check(&aof, 0, 100, 1);
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
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storagePath, "Manifest"));

    dariadb::storage::AOFManager::start(dariadb::storage::AOFManager::Params(storagePath, max_size));

    BOOST_CHECK(dariadb::storage::AOFManager::instance() != nullptr);

    auto aof_files = dariadb::utils::fs::ls(storagePath, dariadb::storage::AOF_FILE_EXT);
    BOOST_CHECK_EQUAL(aof_files.size(),size_t(0));

    dariadb::storage::AOFManager::stop();
    dariadb::storage::Manifest::stop();

    if (dariadb::utils::fs::path_exists(storagePath)) {
      dariadb::utils::fs::rm(storagePath);
    }
}

//BOOST_AUTO_TEST_CASE(CapManager_CommonTest) {
//	const std::string storagePath = "testStorage";
//    const size_t max_size = 5;
//	const dariadb::Time from = 0;
//	const dariadb::Time to = from + 1021;
//	const dariadb::Time step = 10;

//	if (dariadb::utils::fs::path_exists(storagePath)) {
//		dariadb::utils::fs::rm(storagePath);
//	}
//    {
//        dariadb::storage::Manifest::start(
//                    dariadb::utils::fs::append_path(storagePath, "Manifest"));
//        dariadb::storage::CapacitorManager::start(dariadb::storage::CapacitorManager::Params(storagePath, max_size));

//        dariadb_test::storage_test_check(dariadb::storage::CapacitorManager::instance(), from, to, step);

//        dariadb::storage::CapacitorManager::stop();
//        dariadb::storage::Manifest::stop();
//    }
//    {
//		std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
//		stor->writed_count = 0;
//        dariadb::storage::Manifest::start(
//                    dariadb::utils::fs::append_path(storagePath, "Manifest"));
//        dariadb::storage::CapacitorManager::start(dariadb::storage::CapacitorManager::Params(storagePath, max_size));

//        dariadb::storage::QueryInterval qi(dariadb::IdArray{0}, dariadb::Flag(), from, to);
//        dariadb::Meas::MeasList out;
//        dariadb::storage::CapacitorManager::instance()->readInterval(qi)->readAll(&out);
//        BOOST_CHECK_EQUAL(out.size(),dariadb_test::copies_count);


//        auto closed=dariadb::storage::CapacitorManager::instance()->closed_caps();
//		BOOST_CHECK(closed.size() != size_t(0));

//		for (auto fname : closed) {
//            dariadb::storage::CapacitorManager::instance()->drop_cap(fname, stor.get());
//		}
//		BOOST_CHECK(stor->writed_count != size_t(0));

//        closed = dariadb::storage::CapacitorManager::instance()->closed_caps();
//		BOOST_CHECK_EQUAL(closed.size(),size_t(0));

//        dariadb::storage::CapacitorManager::stop();
//        dariadb::storage::Manifest::stop();
//    }
//    if (dariadb::utils::fs::path_exists(storagePath)) {
//      dariadb::utils::fs::rm(storagePath);
//    }
//}
