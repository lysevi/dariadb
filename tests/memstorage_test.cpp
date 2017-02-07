#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main

#include <boost/test/unit_test.hpp>
#include <libdariadb/meas.h>
#include <libdariadb/storage/callbacks.h>
#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/storage/memstorage/memstorage.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <libdariadb/utils/fs.h>

#include <boost/date_time/posix_time/posix_time.hpp>

#include "test_common.h"

struct MokChunkWriter : public dariadb::ChunkContainer {
  size_t droped;

  MokChunkWriter() { droped = 0; }
  ~MokChunkWriter() {}
  using ChunkContainer::foreach;

  void appendChunks(const std::vector<dariadb::storage::Chunk *> &a,
                    size_t count) override {
    droped += count;
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override {
    return false;
  }
  dariadb::Id2Meas
  valuesBeforeTimePoint(const dariadb::QueryTimePoint &q) override {
    return dariadb::Id2Meas{};
  }
  dariadb::Id2Cursor
  intervalReader(const dariadb::QueryInterval &query) override {
    return dariadb::Id2Cursor();
  }

  dariadb::Statistic stat(const dariadb::Id id, dariadb::Time from,
                          dariadb::Time to) override {
    return dariadb::Statistic();
  }
};

struct MocDiskStorage : public dariadb::IMeasWriter {
  size_t droped;
  MocDiskStorage() { droped = 0; }
  virtual dariadb::Status append(const dariadb::Meas &value) override {
    droped++;
    return dariadb::Status(1, 0);
  }

  virtual void flush() override {}
};

BOOST_AUTO_TEST_CASE(MemChunkAllocatorTest) {
  std::cout << "MemChunkAllocatorTest" << std::endl;
  const size_t buffer_size = 100;
  const size_t max_size = 1024;
  dariadb::storage::MemChunkAllocator allocator(max_size, buffer_size);
  std::set<dariadb::storage::ChunkHeader *> allocated_headers;
  std::set<uint8_t *> allocated_buffers;
  std::set<size_t> positions;

  dariadb::storage::MemChunkAllocator::AllocatedData last;
  do {
    auto allocated = allocator.allocate();
    auto hdr = allocated.header;
    auto buf = allocated.buffer;
    auto pos = allocated.position;
    if (hdr == nullptr) {
      break;
    }
    last = allocated;
    allocated_headers.emplace(hdr);
    allocated_buffers.emplace(buf);
    positions.insert(pos);
  } while (1);

  BOOST_CHECK(positions.size() > 0);
  BOOST_CHECK_EQUAL(positions.size(), allocated_headers.size());
  BOOST_CHECK_EQUAL(positions.size(), allocated_buffers.size());

  allocator.free(last);
  auto new_obj = allocator.allocate();
  BOOST_CHECK_EQUAL(new_obj.position, last.position);
}

BOOST_AUTO_TEST_CASE(MemStorageCommonTest) {
  std::cout << "MemStorageCommonTest" << std::endl;
  auto storage_path = "testMemoryStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  {
    auto settings = dariadb::storage::Settings::create(storage_path);
    settings->strategy.setValue(dariadb::STRATEGY::MEMORY);
    settings->chunk_size.setValue(128);
    auto _engine_env = dariadb::storage::EngineEnvironment::create();
    _engine_env->addResource(
        dariadb::storage::EngineEnvironment::Resource::SETTINGS,
        settings.get());
    dariadb::utils::async::ThreadManager::start(
        settings->thread_pools_params());

    auto ms = dariadb::storage::MemStorage::create(_engine_env, size_t(0));

    dariadb_test::storage_test_check(ms.get(), 0, 100, 1, false, true);
  }
  dariadb::utils::async::ThreadManager::stop();
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(MemStorageDropByLimitTest) {
  std::cout << "MemStorageDropByLimitTest" << std::endl;
  auto storage_path = "testMemoryStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  MokChunkWriter *cw = new MokChunkWriter;
  {
    auto settings = dariadb::storage::Settings::create(storage_path);
    dariadb::utils::async::ThreadManager::start(
        settings->thread_pools_params());
    settings->strategy.setValue(dariadb::STRATEGY::MEMORY);
    settings->memory_limit.setValue(1024 * 1024);
    settings->chunk_size.setValue(128);
    auto _engine_env = dariadb::storage::EngineEnvironment::create();
    _engine_env->addResource(
        dariadb::storage::EngineEnvironment::Resource::SETTINGS,
        settings.get());
    dariadb::utils::async::ThreadManager::start(
        settings->thread_pools_params());

    auto ms = dariadb::storage::MemStorage::create(_engine_env, size_t(0));

    ms->setDownLevel(cw);

    auto e = dariadb::Meas();
    while (true) {
      e.time++;
      ms->append(e);
      if (cw->droped != 0) {
        break;
      }
    }
  }
  delete cw;
  dariadb::utils::async::ThreadManager::stop();
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(MemStorageCacheTest) {
  std::cout << "MemStorageCacheTest" << std::endl;
  auto storage_path = "testMemoryStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  MocDiskStorage *cw = new MocDiskStorage;
  {
    auto settings = dariadb::storage::Settings::create(storage_path);
    dariadb::utils::async::ThreadManager::start(
        settings->thread_pools_params());
    settings->strategy.setValue(dariadb::STRATEGY::CACHE);
    settings->memory_limit.setValue(1024 * 1024);
    settings->chunk_size.setValue(128);
    auto _engine_env = dariadb::storage::EngineEnvironment::create();
    _engine_env->addResource(
        dariadb::storage::EngineEnvironment::Resource::SETTINGS,
        settings.get());

    dariadb::utils::async::ThreadManager::start(
        settings->thread_pools_params());

    auto ms = dariadb::storage::MemStorage::create(_engine_env, size_t(0));

    ms->setDiskStorage(cw);

    auto e = dariadb::Meas();
    while (true) {
      e.time++;
      ms->append(e);
      if (cw->droped != 0) {
        break;
      }
    }
  }
  delete cw;
  dariadb::utils::async::ThreadManager::stop();
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(MemStorageWriteToPastTest) {
  std::cout << "MemStorageWriteToPastTest" << std::endl;
  auto storage_path = "testMemoryStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  {
    auto settings = dariadb::storage::Settings::create(storage_path);
    settings->strategy.setValue(dariadb::STRATEGY::MEMORY);
    settings->chunk_size.setValue(128);
    auto _engine_env = dariadb::storage::EngineEnvironment::create();
    _engine_env->addResource(
        dariadb::storage::EngineEnvironment::Resource::SETTINGS,
        settings.get());
    dariadb::utils::async::ThreadManager::start(
        settings->thread_pools_params());

    auto ms = dariadb::storage::MemStorage::create(_engine_env, size_t(0));

    auto meas = dariadb::Meas();

    meas.time = 0;
    for (int i = 0; i < 1024; ++i) {
      auto result = ms->append(meas);
      BOOST_CHECK_EQUAL(result.writed, size_t(1));
      meas.time++;
    }

    meas.time = 4;
    meas.value = 4;
    auto result = ms->append(meas);
    BOOST_CHECK_EQUAL(result.writed, size_t(1));

    auto read_result = ms->readTimePoint(
        dariadb::QueryTimePoint({0}, dariadb::Flag(0), dariadb::Time(4)));

    BOOST_CHECK_EQUAL(read_result[0].time, dariadb::Time(4));
    BOOST_CHECK(dariadb::areSame(read_result[0].value, dariadb::Value(4)));
  }
  dariadb::utils::async::ThreadManager::stop();
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}