#include <gtest/gtest.h>

#include "helpers.h"
#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/storage/wal/wal_manager.h>
#include <libdariadb/storage/wal/walfile.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>

class Moc_Dropper : public dariadb::IWALDropper {
public:
  size_t writed_count;
  std::set<std::string> files;
  dariadb::storage::Settings_ptr _settings;
  dariadb::storage::EngineEnvironment_ptr _env;
  Moc_Dropper(dariadb::storage::Settings_ptr settings,
              dariadb::storage::EngineEnvironment_ptr env) {
    writed_count = 0;
    _settings = settings;
    _env = env;
  }
  void dropWAL(const std::string &fname) override {
    auto full_path = dariadb::utils::fs::append_path(_settings->raw_path.value(), fname);
    auto wal = dariadb::storage::WALFile::open(_env, full_path, true);

    auto ma = wal->readAll();
    wal = nullptr;
    writed_count += ma->size();
    files.insert(fname);
    _env->getResourceObject<dariadb::storage::Manifest>(
            dariadb::storage::EngineEnvironment::Resource::MANIFEST)
        ->wal_rm(fname);
    dariadb::utils::fs::rm(
        dariadb::utils::fs::append_path(_settings->raw_path.value(), fname));
  }
};

TEST(Wal, FileInitTest) {
  const size_t block_size = 1000;
  auto storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  dariadb::utils::fs::mkdir(storage_path);
  auto wal_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::WAL_FILE_EXT);
  assert(wal_files.size() == 0);
  auto settings = dariadb::storage::Settings::create(storage_path);
  settings->wal_cache_size.setValue(block_size);
  settings->wal_file_size.setValue(block_size);

  auto manifest = dariadb::storage::Manifest::create(settings);

  auto _engine_env = dariadb::storage::EngineEnvironment::create();
  _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
                           settings.get());
  _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::MANIFEST,
                           manifest.get());
  size_t writes_count = block_size;

  dariadb::IdSet id_set;
  {
    auto wal = dariadb::storage::WALFile::create(_engine_env);

    wal_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::WAL_FILE_EXT);
    EXPECT_EQ(wal_files.size(), size_t(0));

    auto e = dariadb::Meas();

    size_t id_count = 10;

    size_t i = 0;
    e.id = i % id_count;
    id_set.insert(e.id);
    e.time = dariadb::Time(i);
    e.value = dariadb::Value(i);
    EXPECT_TRUE(wal->append(e).writed == 1);
    i++;
    dariadb::MeasArray ml;
    for (; i < writes_count / 2; i++) {
      e.id = i % id_count;
      id_set.insert(e.id);
      e.time = dariadb::Time(i);
      e.value = dariadb::Value(i);
      ml.push_back(e);
    }
    wal->append(ml.begin(), ml.end());

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
    wal->append(ma.begin(), ma.end());
    wal_files = dariadb::utils::fs::ls(settings->raw_path.value(),
                                       dariadb::storage::WAL_FILE_EXT);
    EXPECT_EQ(wal_files.size(), size_t(1));
    EXPECT_TRUE(wal->id_bloom() != dariadb::Id(0));

    dariadb::MeasArray out;

    out = wal->readInterval(dariadb::QueryInterval(
        dariadb::IdArray(id_set.begin(), id_set.end()), 0, 0, writes_count));
    EXPECT_EQ(out.size(), writes_count);
  }
  {
    wal_files = dariadb::utils::fs::ls(settings->raw_path.value(),
                                       dariadb::storage::WAL_FILE_EXT);
    EXPECT_TRUE(wal_files.size() == size_t(1));
    auto wal = dariadb::storage::WALFile::open(_engine_env, wal_files.front(), true);
    auto all = wal->readAll();
    EXPECT_EQ(all->size(), writes_count);
  }
  manifest = nullptr;

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

TEST(Wal, FileCommonTest) {
  const size_t block_size = 10000;
  auto storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  {
    dariadb::utils::fs::mkdir(storage_path);

    auto settings = dariadb::storage::Settings::create(storage_path);
    settings->wal_cache_size.setValue(block_size);
    settings->wal_file_size.setValue(block_size);

    auto manifest = dariadb::storage::Manifest::create(settings);

    auto _engine_env = dariadb::storage::EngineEnvironment::create();
    _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
                             settings.get());
    _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::MANIFEST,
                             manifest.get());

    auto wal_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::WAL_FILE_EXT);
    EXPECT_TRUE(wal_files.size() == size_t(0));
    auto wal = dariadb::storage::WALFile::create(_engine_env);

    dariadb_test::storage_test_check(wal.get(), 0, 100, 1, false, false, false);
    manifest = nullptr;
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

TEST(Wal, Manager_CommonTest) {
  const std::string storagePath = "testStorage";
  const size_t max_size = 70;
  const dariadb::Time from = 0;
  const dariadb::Time to = from + 1021;
  const dariadb::Time step = 10;

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  dariadb::utils::fs::mkdir(storagePath);
  {
    auto settings = dariadb::storage::Settings::create(storagePath);
    settings->wal_file_size.setValue(size_t(50));
    settings->wal_cache_size.setValue(max_size);
    settings->save();
    auto manifest = dariadb::storage::Manifest::create(settings);

    auto _engine_env = dariadb::storage::EngineEnvironment::create();
    _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
                             settings.get());
    _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::MANIFEST,
                             manifest.get());

    dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

    auto am = dariadb::storage::WALManager::create(_engine_env);

    dariadb_test::storage_test_check(am.get(), from, to, step, false, false, false);

    am = nullptr;
    dariadb::utils::async::ThreadManager::stop();
  }
  {
    auto settings = dariadb::storage::Settings::create(storagePath);

    auto manifest = dariadb::storage::Manifest::create(settings);

    auto _engine_env = dariadb::storage::EngineEnvironment::create();
    _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
                             settings.get());
    _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::MANIFEST,
                             manifest.get());

    dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

    auto am = dariadb::storage::WALManager::create(_engine_env);

    dariadb::QueryInterval qi(dariadb::IdArray{0}, dariadb::Flag(), from, to);
    auto out = am->readInterval(qi);
    EXPECT_EQ(out.size(), dariadb_test::copies_count);

    auto closed = am->closedWals();
    EXPECT_TRUE(closed.size() != size_t(0));
    auto stor = std::make_shared<Moc_Dropper>(settings, _engine_env);
    stor->writed_count = 0;

    for (auto fname : closed) {
      am->dropWAL(fname, stor.get());
    }

    EXPECT_TRUE(stor->writed_count != size_t(0));
    EXPECT_EQ(stor->files.size(), closed.size());

    closed = am->closedWals();
    EXPECT_EQ(closed.size(), size_t(0));

    am = nullptr;
    dariadb::utils::async::ThreadManager::stop();
  }
  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}
