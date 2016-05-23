#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include "test_common.h"
#include <boost/test/unit_test.hpp>

#include <engine.h>
#include <timeutil.h>
#include <utils/fs.h>
#include <utils/logger.h>

class BenchCallback : public dariadb::storage::ReaderClb {
public:
  BenchCallback() { count = 0; }
  void call(const dariadb::Meas &) { count++; }
  size_t count;
};

BOOST_AUTO_TEST_CASE(Engine_common_test) {
  const std::string storage_path = "testStorage";
  const size_t chunk_per_storage = 10000;
  const size_t chunk_size = 256;
  const size_t cap_B = 5;

  const dariadb::Time from = 0;
  const dariadb::Time to = from + 1021;
  const dariadb::Time step = 10;

  {
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    dariadb::storage::MeasStorage_ptr ms{new dariadb::storage::Engine(
        dariadb::storage::PageManager::Params(storage_path, chunk_per_storage,
                                              chunk_size),
        dariadb::storage::Capacitor::Params(cap_B, storage_path),
        dariadb::storage::Engine::Limits(10))};

    dariadb_test::storage_test_check(ms.get(), from, to, step);
  }
  {
    dariadb::storage::MeasStorage_ptr ms{new dariadb::storage::Engine(
        dariadb::storage::PageManager::Params(storage_path, chunk_per_storage,
                                              chunk_size),
        dariadb::storage::Capacitor::Params(cap_B, storage_path),
        dariadb::storage::Engine::Limits(0))};

    dariadb::Meas::MeasList mlist;
    ms->currentValue(dariadb::IdArray{}, 0)->readAll(&mlist);
    BOOST_CHECK(mlist.size() == size_t(1));
    BOOST_CHECK(mlist.front().flag != dariadb::Flags::_NO_DATA);
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}
