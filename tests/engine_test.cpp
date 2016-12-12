#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include "test_common.h"
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <iostream>
#include <libdariadb/engine.h>
#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/storage/page_manager.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>

class BenchCallback : public dariadb::storage::IReaderClb {
public:
  BenchCallback() { count = 0; }
  void call(const dariadb::Meas &) { count++; }
  size_t count;
};

BOOST_AUTO_TEST_CASE(BloomTest) {
  size_t u8_fltr = dariadb::storage::bloom_empty<uint8_t>();

  BOOST_CHECK_EQUAL(u8_fltr, size_t{0});

  u8_fltr = dariadb::storage::bloom_add(u8_fltr, uint8_t{1});
  u8_fltr = dariadb::storage::bloom_add(u8_fltr, uint8_t{2});
  u8_fltr = dariadb::storage::bloom_add(u8_fltr, uint8_t{3});

  BOOST_CHECK(dariadb::storage::bloom_check(u8_fltr, uint8_t{1}));
  BOOST_CHECK(dariadb::storage::bloom_check(u8_fltr, uint8_t{2}));
  BOOST_CHECK(dariadb::storage::bloom_check(u8_fltr, uint8_t{3}));
}

BOOST_AUTO_TEST_CASE(inFilter) {
  {
    auto m = dariadb::Meas::empty();
    m.flag = 100;
    BOOST_CHECK(m.inFlag(0));
    BOOST_CHECK(m.inFlag(100));
    BOOST_CHECK(!m.inFlag(10));
  }
}


BOOST_AUTO_TEST_CASE(Options_Instance) {
  
  const std::string storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  dariadb::utils::fs::mkdir(storage_path);

  auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };

  settings->aof_buffer_size.value = 2;
  settings->chunk_size.value = 7;
  settings->strategy.value = dariadb::storage::STRATEGY::COMPRESSED;
  settings->save();

  settings = nullptr;

  bool file_exists = dariadb::utils::fs::path_exists(
      dariadb::utils::fs::append_path(storage_path, dariadb::storage::SETTINGS_FILE_NAME));
  BOOST_CHECK(file_exists);

  settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
  BOOST_CHECK_EQUAL(settings->aof_buffer_size.value, uint64_t(2));
  BOOST_CHECK_EQUAL(settings->chunk_size.value, uint32_t(7));
  BOOST_CHECK(settings->strategy.value == dariadb::storage::STRATEGY::COMPRESSED);

  settings = nullptr;
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(Engine_common_test) {
  const std::string storage_path = "testStorage";
  const size_t chunk_size = 256;

  const dariadb::Time from = 0;
  const dariadb::Time to = from + 1000;
  const dariadb::Time step = 10;

  using namespace dariadb::storage;

  {
      std::cout<<"Engine_common_test.\n";
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

	auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
    settings->aof_buffer_size.value =100;
    settings->aof_max_size.value = settings->aof_buffer_size.value *5;
    settings->path = storage_path;
    settings->chunk_size.value = chunk_size;
    std::unique_ptr<Engine> ms{new Engine(settings)};

    dariadb_test::storage_test_check(ms.get(), from, to, step, true);
    
    auto pages_count = ms->description().pages_count;
    BOOST_CHECK_GE(pages_count, size_t(2));
  }
  {
    std::cout<<"reopen close storage\n";
	auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };

	auto manifest = dariadb::storage::Manifest_ptr{ new dariadb::storage::Manifest{
		dariadb::utils::fs::append_path(settings->path, "Manifest") } };

    auto manifest_version = manifest->get_version();

	manifest = nullptr;

    auto raw_ptr = new Engine(settings);

    dariadb::storage::IMeasStorage_ptr ms{raw_ptr};
	auto index_files=dariadb::utils::fs::ls(storage_path, ".pagei");
	for (auto&f : index_files) {
		dariadb::utils::fs::rm(f);
	}
    raw_ptr->fsck();

    // check first id, because that Id placed in compressed pages.
    auto values = ms->readInterval(QueryInterval({dariadb::Id(0)}, 0, from, to));
    BOOST_CHECK_EQUAL(values.size(), dariadb_test::copies_count);

    auto current = ms->currentValue(dariadb::IdArray{}, 0);
    BOOST_CHECK(current.size() != size_t(0));
  }
  std::cout<<"end\n";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(Engine_compress_all_test) {
  const std::string storage_path = "testStorage";
  const size_t chunk_size = 256;

  const dariadb::Time from = 0;
  const dariadb::Time to = from + 50;
  const dariadb::Time step = 10;

  using namespace dariadb::storage;

  {
    std::cout<<"Engine_compress_all_test\n";
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
    settings->aof_buffer_size.value =100;
    settings->aof_max_size.value = settings->aof_buffer_size.value *2;
    settings->path = storage_path;
    settings->chunk_size.value = chunk_size;
    settings->strategy.value =dariadb::storage::STRATEGY::FAST_WRITE;
    std::unique_ptr<Engine> ms{new Engine(settings)};

    dariadb::IdSet all_ids;
    dariadb::Time maxWritedTime;
    dariadb_test::fill_storage_for_test(ms.get(), from, to, step,&all_ids, &maxWritedTime);

    ms->compress_all();

    auto pages_count = ms->description().pages_count;
    auto aofs_count = ms->description().aofs_count;
    BOOST_CHECK_GE(pages_count, size_t(1));
    BOOST_CHECK_EQUAL(aofs_count, size_t(0));
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

class Moc_SubscribeClbk : public dariadb::storage::IReaderClb {
public:
  std::list<dariadb::Meas> values;
  void call(const dariadb::Meas &m) override { values.push_back(m); }
  void is_end() override {}
  ~Moc_SubscribeClbk() {}
};

BOOST_AUTO_TEST_CASE(Subscribe) {
  const size_t id_count = 5;
  auto c1=std::make_shared<Moc_SubscribeClbk>();
  auto c2 = std::make_shared<Moc_SubscribeClbk>();
  auto c3 = std::make_shared<Moc_SubscribeClbk>();
  auto c4 = std::make_shared<Moc_SubscribeClbk>();

  const std::string storage_path = "testStorage";
  const size_t chunk_size = 256;

  {
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

	auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
    settings->path = storage_path;
    settings->aof_buffer_size.value = chunk_size;
    settings->chunk_size.value = chunk_size;

    auto ms= std::make_shared<dariadb::storage::Engine>(settings);

    dariadb::IdArray ids{};
    ms->subscribe(ids, 0, c1); // all
    ids.push_back(2);
    ms->subscribe(ids, 0, c2); // 2
    ids.push_back(1);
    ms->subscribe(ids, 0, c3); // 1 2
    ids.clear();
    ms->subscribe(ids, 1, c4); // with flag=1

    auto m = dariadb::Meas::empty();
    const size_t total_count = 100;
    const dariadb::Time time_step = 1;

    for (size_t i = 0; i < total_count; i += time_step) {
      m.id = i % id_count;
      m.flag = dariadb::Flag(i);
      m.time = i;
      m.value = 0;
      ms->append(m);
    }
    BOOST_CHECK_EQUAL(c1->values.size(), total_count);

    BOOST_CHECK_EQUAL(c2->values.size(), size_t(total_count / id_count));
    BOOST_CHECK_EQUAL(c2->values.front().id, dariadb::Id(2));

    BOOST_CHECK_EQUAL(c3->values.size(), size_t(total_count / id_count) * 2);
    BOOST_CHECK_EQUAL(c3->values.front().id, dariadb::Id(1));

    BOOST_CHECK_EQUAL(c4->values.size(), size_t(1));
    BOOST_CHECK_EQUAL(c4->values.front().flag, dariadb::Flag(1));
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}


BOOST_AUTO_TEST_CASE(Engine_MemStorage_common_test) {
	const std::string storage_path = "testStorage";
	const size_t chunk_size = 256;

	const dariadb::Time from = 0;
	const dariadb::Time to = from + 1000;
	const dariadb::Time step = 10;

	using namespace dariadb::storage;

	{
        std::cout<<"Engine_MemStorage_common_test\n";
		if (dariadb::utils::fs::path_exists(storage_path)) {
			dariadb::utils::fs::rm(storage_path);
		}

		auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
        settings->strategy.value = STRATEGY::MEMORY;
		settings->chunk_size.value = chunk_size;
		settings->chunk_size.value = 128;
		settings->memory_limit.value =  50*1024;
		std::unique_ptr<Engine> ms{ new Engine(settings) };

		dariadb_test::storage_test_check(ms.get(), from, to, step, true);

        auto pages_count = ms->description().pages_count;
		BOOST_CHECK_GE(pages_count, size_t(2));
	}
	if (dariadb::utils::fs::path_exists(storage_path)) {
		dariadb::utils::fs::rm(storage_path);
	}
}

BOOST_AUTO_TEST_CASE(Engine_Cache_common_test) {
	const std::string storage_path = "testStorage";

	const size_t chunk_size = 128;
	const dariadb::Time from = 0;
	const dariadb::Time to = from + 1000;
	const dariadb::Time step = 10;

	using namespace dariadb::storage;

	{
		std::cout << "Engine_Cache_common_test\n";
		if (dariadb::utils::fs::path_exists(storage_path)) {
			dariadb::utils::fs::rm(storage_path);
		}

		auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
		settings->strategy.value = STRATEGY::CACHE;
		settings->chunk_size.value = chunk_size;
		settings->memory_limit.value = 50 * 1024;
		settings->aof_max_size.value = 2000;
		std::unique_ptr<Engine> ms{ new Engine(settings) };

		dariadb_test::storage_test_check(ms.get(), from, to, step, true);

		auto descr = ms->description();
		BOOST_CHECK_GT(descr.pages_count, size_t(0));
	}
	if (dariadb::utils::fs::path_exists(storage_path)) {
		dariadb::utils::fs::rm(storage_path);
	}
}
