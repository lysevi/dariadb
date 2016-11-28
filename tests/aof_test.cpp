#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <atomic>
#include <boost/test/unit_test.hpp>
#include <cassert>
#include <map>
#include <thread>

#include "test_common.h"
#include <libdariadb/storage/aof_manager.h>
#include <libdariadb/storage/aofile.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/thread_manager.h>

class Moc_Dropper : public dariadb::storage::IAofDropper {
public:
  size_t writed_count;
  std::set<std::string> files;
  dariadb::storage::Settings_ptr _settings;
  dariadb::storage::EngineEnvironment_ptr _env;
  Moc_Dropper(dariadb::storage::Settings_ptr settings, dariadb::storage::EngineEnvironment_ptr env) { 
	  writed_count = 0; 
	  _settings = settings;
	  _env = env;
  }
  void drop_aof(const std::string fname) override {
    auto full_path = dariadb::utils::fs::append_path(
		_settings->path, fname);
    dariadb::storage::AOFile_Ptr aof{new dariadb::storage::AOFile(_env,full_path, true)};

    auto ma = aof->readAll();
    aof = nullptr;
    writed_count += ma->size();
    files.insert(fname);
	_env->getResourceObject<dariadb::storage::Manifest>(dariadb::storage::EngineEnvironment::Resource::MANIFEST)->aof_rm(fname);
    dariadb::utils::fs::rm(dariadb::utils::fs::append_path(_settings->path, fname));
  }
};

BOOST_AUTO_TEST_CASE(AofInitTest) {
  const size_t block_size = 1000;
  auto storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  dariadb::utils::fs::mkdir(storage_path);
  auto manifest = dariadb::storage::Manifest_ptr{ new dariadb::storage::Manifest{
	  dariadb::utils::fs::append_path(storage_path, "Manifest") } };

  auto aof_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::AOF_FILE_EXT);
  assert(aof_files.size() == 0);
  auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
  settings->aof_buffer_size.value = block_size;
  settings->aof_max_size.value = block_size;

  auto _engine_env = dariadb::storage::EngineEnvironment_ptr{ new dariadb::storage::EngineEnvironment() };
  _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS, settings.get());
  _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::MANIFEST, manifest.get());
  size_t writes_count = block_size;

  dariadb::IdSet id_set;
  {
    dariadb::storage::AOFile aof{ _engine_env };

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
    dariadb::MeasList ml;
    for (; i < writes_count / 2; i++) {
      e.id = i % id_count;
      id_set.insert(e.id);
      e.time = dariadb::Time(i);
      e.value = dariadb::Value(i);
      ml.push_back(e);
    }
    aof.append(ml.begin(), ml.end());

    dariadb::MeasArray ma;
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
    aof.append(ma.begin(), ma.end());
    aof_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::AOF_FILE_EXT);
    BOOST_CHECK_EQUAL(aof_files.size(), size_t(1));

    dariadb::MeasList out;

    out = aof.readInterval(dariadb::storage::QueryInterval(
        dariadb::IdArray(id_set.begin(), id_set.end()), 0, 0, writes_count));
    BOOST_CHECK_EQUAL(out.size(), writes_count);
  }
  {
    aof_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::AOF_FILE_EXT);
    BOOST_CHECK(aof_files.size() == size_t(1));
    dariadb::storage::AOFile aof(_engine_env, aof_files.front(), true);
    auto all = aof.readAll();
    BOOST_CHECK_EQUAL(all->size(), writes_count);
  }
  manifest = nullptr;
  
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
	auto manifest = dariadb::storage::Manifest_ptr{ new dariadb::storage::Manifest{
		dariadb::utils::fs::append_path(storage_path, "Manifest") } };
	auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
    settings->aof_buffer_size.value = block_size;
    settings->aof_max_size.value = block_size;

	auto _engine_env = dariadb::storage::EngineEnvironment_ptr{ new dariadb::storage::EngineEnvironment() };
	_engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS, settings.get());
	_engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::MANIFEST, manifest.get());

    auto aof_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::AOF_FILE_EXT);
    BOOST_CHECK(aof_files.size() == size_t(0));
    dariadb::storage::AOFile aof(_engine_env);

    dariadb_test::storage_test_check(&aof, 0, 100, 1, false);
	manifest = nullptr;
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
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
	  auto manifest = dariadb::storage::Manifest_ptr{ new dariadb::storage::Manifest{
		  dariadb::utils::fs::append_path(storagePath, "Manifest") } };

	auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storagePath) };
    settings->aof_max_size.value = max_size;
    settings->aof_buffer_size.value = max_size;
	
	auto _engine_env = dariadb::storage::EngineEnvironment_ptr{ new dariadb::storage::EngineEnvironment() };
	_engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS, settings.get());
	_engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::MANIFEST, manifest.get());

	dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

	auto am = dariadb::storage::AOFManager_ptr{ new dariadb::storage::AOFManager(_engine_env) };

    dariadb_test::storage_test_check(am.get(), from, to,
                                     step, false);

	am = nullptr;
    dariadb::utils::async::ThreadManager::stop();
  }
  {
	  auto manifest = dariadb::storage::Manifest_ptr{ new dariadb::storage::Manifest{
		  dariadb::utils::fs::append_path(storagePath, "Manifest") } };

	auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storagePath) };
    settings->aof_max_size.value = max_size;
	auto _engine_env = dariadb::storage::EngineEnvironment_ptr{ new dariadb::storage::EngineEnvironment() };
	_engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS, settings.get());
	_engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::MANIFEST, manifest.get());

	dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

	auto am = dariadb::storage::AOFManager_ptr{ new dariadb::storage::AOFManager(_engine_env) };

    dariadb::storage::QueryInterval qi(dariadb::IdArray{0}, dariadb::Flag(), from, to);
    auto out = am->readInterval(qi);
    BOOST_CHECK_EQUAL(out.size(), dariadb_test::copies_count);

    auto closed = am->closed_aofs();
    BOOST_CHECK(closed.size() != size_t(0));
	auto stor = std::make_shared<Moc_Dropper>(settings, _engine_env);
	stor->writed_count = 0;

    for (auto fname : closed) {
      am->drop_aof(fname, stor.get());
    }

    BOOST_CHECK(stor->writed_count != size_t(0));
    BOOST_CHECK_EQUAL(stor->files.size(), closed.size());

    closed = am->closed_aofs();
    BOOST_CHECK_EQUAL(closed.size(), size_t(0));

	am = nullptr;
    dariadb::utils::async::ThreadManager::stop();
  }
  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}
