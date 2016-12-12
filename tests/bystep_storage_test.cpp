#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main

#include <boost/test/unit_test.hpp>
#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/storage/bystep/bystep_storage.h>
#include <libdariadb/storage/callbacks.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/thread_manager.h>

#include <boost/date_time/posix_time/posix_time.hpp>

#include "test_common.h"

BOOST_AUTO_TEST_CASE(ByStepTest) {
	std::cout << "ByStepTest" << std::endl;
	auto storage_path = "testBySTepStorage";
	if (dariadb::utils::fs::path_exists(storage_path)) {
		dariadb::utils::fs::rm(storage_path);
	}
	{
		auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
		settings->chunk_size.value = 128;

		auto _engine_env = dariadb::storage::EngineEnvironment_ptr{ new dariadb::storage::EngineEnvironment() };
		_engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS, settings.get());
		dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

		dariadb::storage::ByStepStorage ms{ _engine_env };

		dariadb_test::storage_test_check(&ms, 0, 100, 1, false);

	}
	dariadb::utils::async::ThreadManager::stop();
	if (dariadb::utils::fs::path_exists(storage_path)) {
		dariadb::utils::fs::rm(storage_path);
	}
}

