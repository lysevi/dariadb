#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include "test_common.h"
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <libdariadb/engine.h>
#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/lock_manager.h>
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

  settings->aof_buffer_size = 2;
  settings->page_chunk_size = 7;
  settings->strategy = dariadb::storage::STRATEGY::COMPRESSED;
  settings->save();

  settings = nullptr;

  bool file_exists = dariadb::utils::fs::path_exists(
      dariadb::utils::fs::append_path(storage_path, dariadb::storage::SETTINGS_FILE_NAME));
  BOOST_CHECK(file_exists);

  settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
  BOOST_CHECK_EQUAL(settings->aof_buffer_size, uint64_t(2));
  BOOST_CHECK_EQUAL(settings->page_chunk_size, uint32_t(7));
  BOOST_CHECK(settings->strategy == dariadb::storage::STRATEGY::COMPRESSED);

  settings = nullptr;
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(Engine_version_test) {
  {
    std::string version = "1.2.3";
    auto parsed = dariadb::storage::Engine::Version::from_string(version);

    BOOST_CHECK_EQUAL(version, parsed.version);
    BOOST_CHECK_EQUAL(1, parsed.major);
    BOOST_CHECK_EQUAL(2, parsed.minor);
    BOOST_CHECK_EQUAL(3, parsed.patch);
  }
  {
    auto v1 = dariadb::storage::Engine::Version::from_string("1.2.3");
    auto v2 = dariadb::storage::Engine::Version::from_string("1.2.4");
    BOOST_CHECK(v2 > v1);
  }

  {
    auto v1 = dariadb::storage::Engine::Version::from_string("1.2.3");
    auto v2 = dariadb::storage::Engine::Version::from_string("1.3.1");
    BOOST_CHECK(v2 > v1);
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
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

	auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
    settings->aof_buffer_size=100;
    settings->aof_max_size = settings->aof_buffer_size*5;
    settings->path = storage_path;
    settings->page_chunk_size = chunk_size;
    std::unique_ptr<Engine> ms{new Engine(settings)};

    auto version = ms->version();
    std::stringstream ss;
    ss << version.major << '.' << version.minor << '.' << version.patch;
    BOOST_CHECK_EQUAL(version.to_string(), ss.str());

    dariadb_test::storage_test_check(ms.get(), from, to, step, true);
    ms->wait_all_asyncs();
    
    auto pages_count = ms->description().pages_count;
    BOOST_CHECK_GE(pages_count, size_t(2));
  }
  {
    
	auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };

	auto manifest = dariadb::storage::Manifest_ptr{ new dariadb::storage::Manifest{
		dariadb::utils::fs::append_path(settings->path, "Manifest") } };

    auto manifest_version = manifest->get_version();

	manifest = nullptr;

    auto raw_ptr = new Engine(settings);
    {
        auto m=dariadb::Meas::empty();
        m.time=from;
        auto res=raw_ptr->append(m);
        BOOST_CHECK_EQUAL(res.ignored,size_t(1));
        BOOST_CHECK_GE(res.error_message.size(),size_t(0));
    }
    auto version = raw_ptr->version();
    std::stringstream ss;
    ss << version.major << '.' << version.minor << '.' << version.patch;
    BOOST_CHECK_EQUAL(manifest_version, ss.str());

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
    settings->aof_buffer_size = chunk_size;
    settings->page_chunk_size = chunk_size;

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
		if (dariadb::utils::fs::path_exists(storage_path)) {
			dariadb::utils::fs::rm(storage_path);
		}

		auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
        settings->strategy = STRATEGY::MEMORY;
		settings->page_chunk_size = chunk_size;
		settings->page_chunk_size = 128;
		settings->memory_limit =  50*1024;
		std::unique_ptr<Engine> ms{ new Engine(settings) };

		auto version = ms->version();
		std::stringstream ss;
		ss << version.major << '.' << version.minor << '.' << version.patch;
		BOOST_CHECK_EQUAL(version.to_string(), ss.str());

		dariadb_test::storage_test_check(ms.get(), from, to, step, true);
		ms->wait_all_asyncs();

        auto pages_count = ms->description().pages_count;
		BOOST_CHECK_GE(pages_count, size_t(2));
	}
	if (dariadb::utils::fs::path_exists(storage_path)) {
		dariadb::utils::fs::rm(storage_path);
	}
}
