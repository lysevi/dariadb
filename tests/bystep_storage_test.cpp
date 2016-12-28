#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include "test_common.h"
#include <libdariadb/storage/bystep/bystep_storage.h>
#include <libdariadb/storage/bystep/io_adapter.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/async/thread_manager.h>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <cstring>
#include <iostream>

BOOST_AUTO_TEST_CASE(ByStepIntervalCalculationTest) {
	using namespace dariadb::storage;
	BOOST_CHECK_EQUAL(ByStepStorage::intervalForTime(STEP_KIND::SECOND, 1, 0777), uint64_t(0));
	BOOST_CHECK_EQUAL(ByStepStorage::intervalForTime(STEP_KIND::SECOND, 1, 1011), uint64_t(1));
	BOOST_CHECK_EQUAL(ByStepStorage::intervalForTime(STEP_KIND::SECOND, 10, 1000), uint64_t(0));
	BOOST_CHECK_EQUAL(ByStepStorage::intervalForTime(STEP_KIND::SECOND, 10,20111), uint64_t(2));
	BOOST_CHECK_EQUAL(ByStepStorage::intervalForTime(STEP_KIND::SECOND, 60, 60000), uint64_t(1));

	BOOST_CHECK_EQUAL(ByStepStorage::intervalForTime(STEP_KIND::MINUTE, 1, 1 * 1000), uint64_t(0));
	BOOST_CHECK_EQUAL(ByStepStorage::intervalForTime(STEP_KIND::MINUTE, 1, 65*1000), uint64_t(1));
}

BOOST_AUTO_TEST_CASE(IOAdapterTest) {
  std::cout << "IOAdopterInitTest" << std::endl;
  auto storage_path = "testBySTepStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  dariadb::utils::fs::mkdir(storage_path);
  auto fname = dariadb::utils::fs::append_path(storage_path, "io_adapter.db");
  {
    const int insertion_count = 10;
	auto settings =
		dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
	settings->chunk_size.setValue(128);

	auto _engine_env = dariadb::storage::EngineEnvironment_ptr{
		new dariadb::storage::EngineEnvironment() };
	_engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
		settings.get());
	dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

    auto io_adapter = std::make_unique<dariadb::storage::IOAdapter>(fname);
    BOOST_CHECK_EQUAL(dariadb::utils::fs::ls(storage_path, ".db").size(), size_t(1));


    size_t buffer_size = 16;
    
    dariadb::IdSet all_ids;
    dariadb::Time max_time = 0;
    size_t created_chunks = 0;
	dariadb::MeasList all_values;
    for (int i = 0; i < insertion_count; ++i) {
      auto first = dariadb::Meas::empty(i);
      all_ids.insert(i);
      for (size_t j = 0; j < 3; ++j) {
		  dariadb::storage::ChunkHeader *hdr=new dariadb::storage::ChunkHeader;
		  uint8_t *buffer = new uint8_t[buffer_size];

		  memset(hdr, 0, sizeof(dariadb::storage::ChunkHeader));
		  memset(buffer, 0, buffer_size);

        dariadb::storage::Chunk_Ptr ptr{
            new dariadb::storage::ZippedChunk(hdr, buffer, buffer_size, first)};
		all_values.push_back(first);
        while (!ptr->isFull()) {
          first.time++;
          first.value++;
          first.flag++;
		  if (ptr->append(first)) {
			  all_values.push_back(first);
		  }
          max_time = std::max(max_time, first.time);
        }
		ptr->header->id = j;
		ptr->is_owner = true;
        ptr->close();
        created_chunks++;
        io_adapter->append(ptr, ptr->header->minTime, ptr->header->maxTime);
      }
    }
	
	{//readInterval
		auto all_chunks = io_adapter->readInterval(0,2,0);
		BOOST_CHECK_EQUAL(all_chunks.size(), 3);

		dariadb::MeasList readed_values;
		for (auto&c : all_chunks) {
			auto rdr = c->getReader();
			while (!rdr->is_end()) {
				auto v = rdr->readNext();
				readed_values.push_back(v);
			}
		}
		size_t count_of_zero = std::count_if(all_values.begin(), all_values.end(), [](auto v) {return v.id == 0; });
		BOOST_CHECK_EQUAL(readed_values.size(), count_of_zero);
	}
	{//readTimePoint
		dariadb::Time tp = max_time / 2;
		auto result = io_adapter->readTimePoint(1,0);
		BOOST_CHECK(result != nullptr);
		auto from = result->header->minTime;
		auto to = result->header->maxTime;
		BOOST_CHECK(dariadb::utils::inInterval(from, to, tp));
	}
	{//current value
		auto values = io_adapter->currentValue();
		BOOST_CHECK_EQUAL(values.size(), size_t(insertion_count));
	}
	//replace chunk
	{
		dariadb::storage::ChunkHeader *hdr=new dariadb::storage::ChunkHeader;
		uint8_t *buffer = new uint8_t[buffer_size];
		memset(hdr, 0, sizeof(dariadb::storage::ChunkHeader));
		memset(buffer, 0, buffer_size);

		auto first = dariadb::Meas::empty(0);
		first.time = 1000;
		dariadb::storage::Chunk_Ptr ptr{ new dariadb::storage::ZippedChunk(hdr, buffer, buffer_size, first) };
		while (!ptr->isFull()) {
			first.time++;
			first.value++;
			first.flag++;
			if (ptr->append(first)) {
				max_time = std::max(max_time, first.time);
			}
		}
		ptr->header->id = 0;
		ptr->is_owner = true;
		ptr->close();
		io_adapter->replace(ptr, ptr->header->minTime,ptr->header->maxTime);

		auto zero_id_chunks = io_adapter->readInterval(0,3,0);
		bool success_replace = false;
		for (auto&c : zero_id_chunks) {
			auto rdr = c->getReader();
			while (!rdr->is_end()) {
				auto v = rdr->readNext();
				if (v.time >= 1000) {
					success_replace = true;
					break;
				}
			}
		}
		BOOST_CHECK(success_replace);
	}


	{//minMax
		auto minTime = io_adapter->minTime();
		auto maxTime = io_adapter->maxTime();
		BOOST_CHECK_LT(minTime, maxTime);
	}

	{//minMax for id
		dariadb::Time minTime, maxTime;
		auto true_val=io_adapter->minMaxTime(0,&minTime,&maxTime);
		BOOST_CHECK(true_val);
		BOOST_CHECK_LT(minTime, maxTime);
	}
	{//erase
		io_adapter->eraseOld(0, 2, 0);
		auto all_chunks = io_adapter->readInterval(0, 2, 0);
		BOOST_CHECK_LT(all_chunks.size(), 3);
	}
  }
  dariadb::utils::async::ThreadManager::stop();
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
    settings->chunk_size.setValue(128);

    auto _engine_env = dariadb::storage::EngineEnvironment_ptr{
        new dariadb::storage::EngineEnvironment()};
    _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
                             settings.get());
    dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

    dariadb::storage::ByStepStorage ms{_engine_env};
  }
  dariadb::utils::async::ThreadManager::stop();
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(ByStepAppendTest) {
  std::cout << "ByStepTest" << std::endl;
  auto storage_path = "testBySTepStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  auto value = dariadb::Meas::empty(0);
  size_t writes_count = 10000;
  {
    dariadb::utils::fs::mkdir(storage_path);
    auto settings =
        dariadb::storage::Settings_ptr{new dariadb::storage::Settings(storage_path)};

    auto _engine_env = dariadb::storage::EngineEnvironment_ptr{
        new dariadb::storage::EngineEnvironment()};
    _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
                             settings.get());
    dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

    dariadb::storage::ByStepStorage ms{_engine_env};
    dariadb::storage::Id2Step steps;

    steps[0] = dariadb::storage::STEP_KIND::SECOND;
    steps[1] = dariadb::storage::STEP_KIND::MINUTE;
    steps[2] = dariadb::storage::STEP_KIND::HOUR;
    ms.setSteps(steps);

    
    for (size_t i = 0; i < writes_count; i++) {
      value.id = 0;
      value.value = i;
      value.time += 500;
      ms.append(value);
      value.id = 1;
      ms.append(value);
      value.id = 2;
      ms.append(value);
    }
    { // seconds
      dariadb::storage::QueryInterval qi({0}, 0, 0, value.time);
      auto readed = ms.readInterval(qi);
      BOOST_CHECK_EQUAL(readed.size(), size_t(writes_count / 2) + 1);
      for (auto &v : readed) {
        BOOST_CHECK(v.flag != dariadb::Flags::_NO_DATA);
      }
    }

    { // seconds no flag
      dariadb::storage::QueryInterval qi({0}, 777, 0, value.time);
      auto readed = ms.readInterval(qi);
      BOOST_CHECK_EQUAL(readed.size(), size_t(writes_count / 2) + 1);
      for (auto &v : readed) {
        BOOST_CHECK(v.flag == dariadb::Flags::_NO_DATA);
      }
    }

    { // minutes
      dariadb::storage::QueryInterval qi({1}, 0, 0, value.time);
      auto readed = ms.readInterval(qi);
      BOOST_CHECK_EQUAL(readed.size(), size_t(writes_count / (2 * 60)) + 1);
      for (auto &v : readed) {
        BOOST_CHECK(v.flag != dariadb::Flags::_NO_DATA);
      }
    }

    { // hour
      dariadb::storage::QueryInterval qi({2}, 0, 0, value.time);
      auto readed = ms.readInterval(qi);
      BOOST_CHECK_EQUAL(readed.size(), size_t(writes_count / (2 * 60 * 60)) + 1);
      for (auto &v : readed) {
        BOOST_CHECK(v.flag != dariadb::Flags::_NO_DATA);
      }
    }

    { // minMax
      auto min = ms.minTime();
      BOOST_CHECK_EQUAL(min, 500);
      auto max = ms.maxTime();
      BOOST_CHECK_EQUAL(max, value.time);

      auto res = ms.minMaxTime(0, &min, &max);
      BOOST_CHECK(res);
      BOOST_CHECK_EQUAL(min, 500);
      BOOST_CHECK_EQUAL(max, value.time);

      res = ms.minMaxTime(777, &min, &max);
      BOOST_CHECK(!res);
    }

    { // readInTimePoint from io_adapter
      dariadb::storage::QueryTimePoint qp({0}, 0, 1100);
      auto result = ms.readTimePoint(qp);
      BOOST_CHECK_EQUAL(result.size(), size_t(1));
      BOOST_CHECK_LE(result[0].time, 2000);
      BOOST_CHECK(result[0].flag != dariadb::Flags::_NO_DATA);
    }
    { // readInTimePoint from last tracked value
      dariadb::storage::QueryTimePoint qp({0}, 0, value.time);
      auto result = ms.readTimePoint(qp);
      BOOST_CHECK_EQUAL(result.size(), size_t(1));
      BOOST_CHECK_LE(result[0].time, value.time);
      BOOST_CHECK(result[0].flag != dariadb::Flags::_NO_DATA);
    }

    { // readInTimePoint not in interval
      auto not_exists_time = value.time * value.time;
      dariadb::storage::QueryTimePoint qp({1}, 0, not_exists_time);
      auto result = ms.readTimePoint(qp);
      BOOST_CHECK_EQUAL(result.size(), size_t(1));
      BOOST_CHECK_LE(result[1].time, not_exists_time);
      BOOST_CHECK_EQUAL(result[1].flag, dariadb::Flags::_NO_DATA);
    }

    { // query not exists interval
      dariadb::storage::QueryInterval qi({0}, 0, value.time * 2, value.time * 10);
      auto readed = ms.readInterval(qi);
      BOOST_CHECK(readed.size() != size_t(0));
      for (auto &v : readed) {
        BOOST_CHECK_EQUAL(v.flag, dariadb::Flags::_NO_DATA);
      }
    }
    { // write to past
      auto new_value = dariadb::Meas::empty(0);
      new_value.time = 500;
      new_value.value = 777;
      ms.append(new_value);
      dariadb::storage::QueryInterval qi({0}, 0, 0, value.time);
      auto readed = ms.readInterval(qi);
      BOOST_CHECK_EQUAL(readed.size(), size_t(writes_count / 2) + 1);
      BOOST_CHECK_EQUAL(readed.front().value, 777);
    }
  }
  {
    dariadb::utils::fs::mkdir(storage_path);
    auto settings =
        dariadb::storage::Settings_ptr{new dariadb::storage::Settings(storage_path)};

    auto _engine_env = dariadb::storage::EngineEnvironment_ptr{
        new dariadb::storage::EngineEnvironment()};
    _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
                             settings.get());
    dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

    dariadb::storage::ByStepStorage ms{_engine_env};
    dariadb::storage::Id2Step steps;

    steps[0] = dariadb::storage::STEP_KIND::SECOND;
    steps[1] = dariadb::storage::STEP_KIND::MINUTE;
    steps[2] = dariadb::storage::STEP_KIND::HOUR;
    ms.setSteps(steps);

	{ // write to past
		auto new_value = dariadb::Meas::empty(0);
		new_value.time = 500;
		new_value.value = 888;
		ms.append(new_value);
		dariadb::storage::QueryInterval qi({ 0 }, 0, 0, value.time);
		auto readed = ms.readInterval(qi);
		BOOST_CHECK_EQUAL(readed.front().value, 888);
	}

	{//erase
		ms.eraseOld(0, 0, 3601000*10);
		dariadb::storage::QueryInterval qi({ 0 }, 0, 0, value.time);
		auto readed = ms.readInterval(qi);
		BOOST_CHECK_LT(readed.size(), size_t(writes_count / 2) + 1);
	}
  }
  dariadb::utils::async::ThreadManager::stop();
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(ByStepLoadOnStartTest) {
  std::cout << "ByStepLoadOnStartTest" << std::endl;
  auto storage_path = "testBySTepStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  {
    dariadb::utils::fs::mkdir(storage_path);
    auto settings =
        dariadb::storage::Settings_ptr{new dariadb::storage::Settings(storage_path)};

    auto _engine_env = dariadb::storage::EngineEnvironment_ptr{
        new dariadb::storage::EngineEnvironment()};
    _engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
                             settings.get());
    dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

    dariadb::storage::Id2Step steps;
    steps[0] = dariadb::storage::STEP_KIND::HOUR;
    {
      dariadb::storage::ByStepStorage ms{_engine_env};
      ms.setSteps(steps);

      auto value = dariadb::Meas::empty(0);
      value.value = 1;
      value.time = dariadb::timeutil::current_time();
      ms.append(value);

      { // query
        dariadb::storage::QueryTimePoint qt({0}, 0, value.time);
        auto readed = ms.readTimePoint(qt);
        BOOST_CHECK_EQUAL(readed.size(), size_t(1));
        BOOST_CHECK_EQUAL(readed[0].value, 1);
      }
    }
    { // update value
      dariadb::storage::ByStepStorage ms{_engine_env};
      auto readed = ms.setSteps(steps);
      BOOST_CHECK_EQUAL(readed, 1);
      auto value = dariadb::Meas::empty(0);
      value.value = 2;
      value.time = dariadb::timeutil::current_time();
      ms.append(value);

      { // query
        dariadb::storage::QueryTimePoint qt({0}, 0, value.time);
        auto readed_values = ms.readTimePoint(qt);
        BOOST_CHECK_EQUAL(readed_values.size(), size_t(1));
        BOOST_CHECK_EQUAL(readed_values[0].value, 2);
      }
    }

    { // read saved
      dariadb::storage::ByStepStorage ms{_engine_env};
      auto readed = ms.setSteps(steps);
      BOOST_CHECK_EQUAL(readed, 1);
      auto curtime = dariadb::timeutil::current_time();

      { // query
        dariadb::storage::QueryTimePoint qt({0}, 0, curtime);
        auto readed_values = ms.readTimePoint(qt);
        BOOST_CHECK_EQUAL(readed_values.size(), size_t(1));
        BOOST_CHECK_EQUAL(readed_values[0].value, 2);
      }
    }	
  }
  dariadb::utils::async::ThreadManager::stop();
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}