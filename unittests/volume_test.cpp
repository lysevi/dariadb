
#include <catch.hpp>

#include "helpers.h"

#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/storage/volume.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/utils.h>

class MockFlushable final : public dariadb::storage::IFlushable {
public:
  // Inherited via IFlushable
  void flush(uint8_t *, size_t) override { calls++; }
  size_t calls = 0;
};

TEST_CASE("Volume.Index") {
  using namespace dariadb::storage;
  VolumeIndex::Param p{uint8_t(3), uint8_t(5)};
  auto sz = VolumeIndex::index_size(p);
  EXPECT_GT(sz, size_t(0));

  uint8_t *buffer = new uint8_t[sz + 1];
  buffer[sz] = uint8_t(255);
  uint64_t address = 1, chunk_id = 1;
  size_t addeded = 0;
  dariadb::Time maxTime = 1;
  dariadb::Id targetId{10};
  auto flushable = std::make_unique<MockFlushable>();
  {
    VolumeIndex c(p, flushable.get(), targetId, buffer);
    EXPECT_EQ(buffer[sz], uint8_t(255));
    EXPECT_EQ(c.levels(), p.levels);
    EXPECT_EQ(c.targetId(), targetId);

    auto lnk = c.queryLink(maxTime);
    EXPECT_TRUE(lnk.IsEmpty());

    while (c.addLink(address, chunk_id, maxTime)) {
      addeded++;
      auto queryResult = c.queryLink(dariadb::Time(1), maxTime);
      EXPECT_EQ(addeded, queryResult.size());
      EXPECT_EQ(buffer[sz], uint8_t(255));

      bool sorted =
          std::is_sorted(queryResult.begin(), queryResult.end(),
                         [](const VolumeIndex::Link &l, const VolumeIndex::Link &r) {
                           return l.max_time < r.max_time;
                         });
      EXPECT_TRUE(sorted);

      lnk = c.queryLink(maxTime);
      EXPECT_EQ(lnk.max_time, maxTime);

      lnk = c.queryLink(maxTime + 1);
      EXPECT_EQ(lnk.max_time, maxTime);

      if (maxTime != size_t(1)) {
        lnk = c.queryLink(maxTime - 1);
        EXPECT_EQ(lnk.max_time, maxTime - 1);
      }
      auto mm = c.minMax();
      EXPECT_EQ(mm.second.max_time, maxTime);

      address++;
      chunk_id++;
      maxTime++;
    }
    EXPECT_GT(flushable->calls, size_t(0));
    EXPECT_GT(addeded, size_t(0));
    auto queryResult = c.queryLink(dariadb::Time(1), maxTime);
    EXPECT_EQ(addeded, queryResult.size());
  }
  {
    VolumeIndex c(flushable.get(), buffer);
    EXPECT_EQ(c.levels(), p.levels);
    EXPECT_EQ(c.targetId(), targetId);
    auto queryResult = c.queryLink(dariadb::Time(1), maxTime);
    auto size_on_start = queryResult.size();
    EXPECT_EQ(addeded, size_on_start);

    for (size_t i = addeded;; --i) {
      if (i == 0 || size_on_start == 0) {
        break;
      }
      c.rm(dariadb::Time(addeded), uint64_t(addeded));
      addeded--;

      queryResult = c.queryLink(dariadb::Time(1), maxTime);
      EXPECT_GT(size_on_start, queryResult.size());
      size_on_start = queryResult.size();
    }
  }
  delete[] buffer;
}

dariadb::storage::Chunk_Ptr createChunk(dariadb::storage::ChunkHeader *hdr,
                                        uint8_t *buffer, size_t chunk_sz, dariadb::Id id,
                                        dariadb::Time startTime) {
  using namespace dariadb;
  using namespace dariadb::storage;

  Meas m = Meas(id);
  m.time = startTime;
  m.value = Value(10);
  auto ch = dariadb::storage::Chunk::create(hdr, buffer, uint32_t(chunk_sz), m);
  while (ch->append(m)) {
    m.time++;
    m.value++;
  }
  return ch;
}

TEST_CASE("Volume.Init") {
  using namespace dariadb::storage;
  using namespace dariadb::utils;
  using namespace dariadb;

  const size_t chunk_size = 100;
  auto storage_path = "testStorage";

  if (fs::path_exists(storage_path)) {
    fs::rm(storage_path);
  }
  fs::mkdir(storage_path);

  auto file_path = fs::append_path(storage_path, "volume1.vlm");

  const Id idCount = 10;

  Time t = 0;
  uint64_t chunkId = 0;
  bool isFull = false;
  std::unordered_map<dariadb::Id, size_t> id2chunks_count;

  { // create
    EXPECT_TRUE(fs::ls(storage_path).empty());
    Volume::Params params(1024 * 10, chunk_size, 3, 1);
    Volume vlm(params, file_path, FlushModel::NOT_SAFETY);

    EXPECT_EQ(fs::ls(storage_path).size(), size_t(1));

    while (!isFull) {
      for (Id id = 0; id < idCount && !isFull; ++id) {
        for (size_t i = 0; i < 2 && !isFull; ++i) {
          ChunkHeader chdr;

          uint8_t *buffer = new uint8_t[chunk_size];
          auto c = createChunk(&chdr, buffer, chunk_size, id, t);
          t = c->header->stat.maxTime;
          c->header->id = ++chunkId;

          isFull = vlm.addChunk(c).error != dariadb::APPEND_ERROR::OK;
          delete[] buffer;

          if (!isFull) {
            auto mm = vlm.loadMinMax();
            EXPECT_EQ(mm[id].first.id, id);
            EXPECT_EQ(mm[id].second.id, id);
            EXPECT_EQ(mm[id].second.time, t);
            id2chunks_count[id] += size_t(1);
            auto chunks = vlm.queryChunks(id, MIN_TIME, t);
            EXPECT_EQ(chunks.size(), id2chunks_count[id]);

            for (auto ch : chunks) {
              auto rdr = ch->getReader();
              while (!rdr->is_end()) {
                rdr->readNext();
              }
            }

            auto singleC = vlm.queryChunks(id, t + 1);
            EXPECT_TRUE(singleC != nullptr);
            EXPECT_EQ(singleC->header->stat.maxTime, t);
            auto rdr = singleC->getReader();
            while (!rdr->is_end()) {
              rdr->readNext();
            }
          }
        }
      }
    }
    auto ids = vlm.indexes();
    EXPECT_EQ(ids.size(), id2chunks_count.size());
    for (auto id : ids) {
      EXPECT_TRUE(id2chunks_count.find(id) != id2chunks_count.end());
    }
  }
  {
    Volume vlm(file_path, FlushModel::NOT_SAFETY);
    auto ids = vlm.indexes();
    EXPECT_EQ(ids.size(), id2chunks_count.size());
    for (auto id : ids) {
      EXPECT_TRUE(id2chunks_count.find(id) != id2chunks_count.end());
    }

    for (auto kv : id2chunks_count) {
      auto chunks = vlm.queryChunks(kv.first, MIN_TIME, t);
      EXPECT_EQ(chunks.size(), kv.second);
      for (auto c : chunks) {
        auto rdr = c->getReader();
        while (!rdr->is_end()) {
          rdr->readNext();
        }
      }

      auto singleC = vlm.queryChunks(kv.first, t);
      EXPECT_TRUE(singleC != nullptr);
      EXPECT_LE(singleC->header->stat.maxTime, t);
      auto rdr = singleC->getReader();
      while (!rdr->is_end()) {
        rdr->readNext();
      }
    }
  }

  if (fs::path_exists(storage_path)) {
    fs::rm(storage_path);
  }
}

TEST_CASE("Volume.Manager") {
  using namespace dariadb::storage;
  using namespace dariadb::utils;
  using namespace dariadb;

  const size_t chunk_size = 50;
  auto storage_path = "testStorage";

  if (fs::path_exists(storage_path)) {
    fs::rm(storage_path);
  }
  fs::mkdir(storage_path);

  const Id idCount = 10;

  auto settings = dariadb::storage::Settings::create(storage_path);
  settings->chunk_size.setValue(chunk_size);
  settings->volume_size.setValue(1024 * 50);
  settings->volume_levels_count.setValue(3);
  settings->volume_B.setValue(2);

  auto manifest = dariadb::storage::Manifest::create(settings);

  auto _engine_env = dariadb::storage::EngineEnvironment::create();
  _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
                           settings.get());
  _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::MANIFEST,
                           manifest.get());

  dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

  SECTION("ManagerCommon") {
    auto vlm = VolumeManager::create(_engine_env);
    dariadb_test::storage_test_check(vlm.get(), 0, 100, 1, false, false, false);
    auto files = fs::ls(settings->raw_path.value());
    EXPECT_TRUE(files.size() > size_t(1));
  }

  manifest = nullptr;
  dariadb::utils::async::ThreadManager::stop();

  if (fs::path_exists(storage_path)) {
    fs::rm(storage_path);
  }
}