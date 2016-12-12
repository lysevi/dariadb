#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main

#include <libdariadb/storage/bystep/bystep_storage.h>
#include <libdariadb/storage/bystep/io_adapter.h>
#include <libdariadb/storage/callbacks.h>
#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/thread_manager.h>
#include <boost/test/unit_test.hpp>

#include <boost/date_time/posix_time/posix_time.hpp>

#include "test_common.h"

#include <libsqlite3/sqlite3.h>

BOOST_AUTO_TEST_CASE(IOAdopterInitTest) {
  std::cout << "IOAdopterInitTest" << std::endl;
  auto storage_path = "testBySTepStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  dariadb::utils::fs::mkdir(storage_path);
  auto fname = dariadb::utils::fs::append_path(storage_path, "io_adapter.db");
  {
    auto io_adapter = std::make_unique<dariadb::storage::IOAdapter>(fname);
    BOOST_CHECK_EQUAL(dariadb::utils::fs::ls(storage_path, ".db").size(), size_t(1));
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(ByStepInitTest) {
  std::cout << "ByStepTest" << std::endl;
  auto storage_path = "testBySTepStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  {
    dariadb::utils::fs::mkdir(storage_path);
    auto settings =
        dariadb::storage::Settings_ptr{new dariadb::storage::Settings(storage_path)};
    settings->chunk_size.value = 128;

    auto _engine_env = dariadb::storage::EngineEnvironment_ptr{
        new dariadb::storage::EngineEnvironment()};
    _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
                             settings.get());
    dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

    dariadb::storage::ByStepStorage ms{_engine_env};
    BOOST_CHECK_EQUAL(dariadb::utils::fs::ls(storage_path, ".db").size(), size_t(1));
  }
  dariadb::utils::async::ThreadManager::stop();
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}
