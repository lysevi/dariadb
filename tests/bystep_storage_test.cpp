#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include "test_common.h"
#include <libdariadb/storage/bystep/bystep_storage.h>
#include <libdariadb/storage/bystep/io_adapter.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/thread_manager.h>

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <cstring>

BOOST_AUTO_TEST_CASE(ByStepIntervalCalculationTest) {
	using namespace dariadb::storage;
	BOOST_CHECK_EQUAL(ByStepStorage::intervalForTime(StepKind::SECOND, 1, 0777), uint64_t(0));
	BOOST_CHECK_EQUAL(ByStepStorage::intervalForTime(StepKind::SECOND, 1, 1011), uint64_t(1));
	BOOST_CHECK_EQUAL(ByStepStorage::intervalForTime(StepKind::SECOND, 10, 1000), uint64_t(0));
	BOOST_CHECK_EQUAL(ByStepStorage::intervalForTime(StepKind::SECOND, 10,20111), uint64_t(2));
	BOOST_CHECK_EQUAL(ByStepStorage::intervalForTime(StepKind::SECOND, 60, 60000), uint64_t(1));

	BOOST_CHECK_EQUAL(ByStepStorage::intervalForTime(StepKind::MINUTE, 1, 1 * 1000), uint64_t(0));
	BOOST_CHECK_EQUAL(ByStepStorage::intervalForTime(StepKind::MINUTE, 1, 65*1000), uint64_t(1));
}


BOOST_AUTO_TEST_CASE(IOAdopterInitTest) {
  std::cout << "IOAdopterInitTest" << std::endl;
  auto storage_path = "testBySTepStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  dariadb::utils::fs::mkdir(storage_path);
  auto fname = dariadb::utils::fs::append_path(storage_path, "io_adapter.db");
  {
    const int insertion_count = 10;
    auto io_adapter = std::make_unique<dariadb::storage::IOAdapter>(fname);
    BOOST_CHECK_EQUAL(dariadb::utils::fs::ls(storage_path, ".db").size(), size_t(1));

    dariadb::storage::ChunkHeader hdr;
    size_t buffer_size = 16;
    uint8_t *buffer = new uint8_t[buffer_size];
    dariadb::IdSet all_ids;
    dariadb::Time max_time = 0;
    size_t created_chunks = 0;
	dariadb::MeasList all_values;
    for (int i = 0; i < insertion_count; ++i) {
      memset(&hdr, 0, sizeof(dariadb::storage::ChunkHeader));
      memset(buffer, 0, buffer_size);

      hdr.id = i;
      auto first = dariadb::Meas::empty(i);
      all_ids.insert(i);
      for (size_t j = 0; j < 3; ++j) {
        dariadb::storage::Chunk_Ptr ptr{
            new dariadb::storage::ZippedChunk(&hdr, buffer, buffer_size, first)};
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
        ptr->close();
        created_chunks++;
        io_adapter->append(ptr);
      }
    }
    
	{//readInterval
		dariadb::storage::QueryInterval qi{ dariadb::IdArray{all_ids.begin(), all_ids.end()},
										   0, 0, max_time };
		auto all_chunks = io_adapter->readInterval(qi);
		BOOST_CHECK_EQUAL(all_chunks.size(), created_chunks);

		dariadb::MeasList readed_values;
		for (auto&c : all_chunks) {
			auto rdr = c->getReader();
			while (!rdr->is_end()) {
				auto v = rdr->readNext();
				readed_values.push_back(v);
			}
		}
		BOOST_CHECK_EQUAL(readed_values.size(), all_values.size());
	}
	{//readTimePoint
		dariadb::Time tp = max_time / 2;
		dariadb::storage::QueryTimePoint qtp{ dariadb::IdArray{all_ids.begin(), all_ids.end()},
											 0, tp };

		auto tp_chunks = io_adapter->readTimePoint(qtp);
		BOOST_CHECK_EQUAL(tp_chunks.size(), all_ids.size());
		for (auto &kv : tp_chunks) {
			auto from = kv.second->header->minTime;
			auto to = kv.second->header->maxTime;
			BOOST_CHECK(dariadb::utils::inInterval(from, to, tp));
		}
	}
	//replace chunk
	{
		memset(&hdr, 0, sizeof(dariadb::storage::ChunkHeader));
		memset(buffer, 0, buffer_size);

		auto first = dariadb::Meas::empty(0);
		first.time = 1000;
		dariadb::storage::Chunk_Ptr ptr{ new dariadb::storage::ZippedChunk(&hdr, buffer, buffer_size, first) };
		while (!ptr->isFull()) {
			first.time++;
			first.value++;
			first.flag++;
			if (ptr->append(first)) {
				max_time = std::max(max_time, first.time);
			}
		}
		ptr->header->id = 0;
		ptr->close();
		io_adapter->replace(ptr);

		dariadb::IdArray zero(1);
		zero[0] = 0;
		dariadb::storage::QueryInterval qi_zero{ zero, 0, 0, first.time };

		auto zero_id_chunks = io_adapter->readInterval(qi_zero);
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
	delete[] buffer;

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
	{
		dariadb::utils::fs::mkdir(storage_path);
		auto settings =
			dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };

		auto _engine_env = dariadb::storage::EngineEnvironment_ptr{
			new dariadb::storage::EngineEnvironment() };
		_engine_env->addResource(dariadb::storage::EngineEnvironment::Resource::SETTINGS,
			settings.get());
		dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());

		dariadb::storage::ByStepStorage ms{ _engine_env };
		dariadb::storage::Id2Step steps;
		
		steps[0] = dariadb::storage::StepKind::SECOND;
		steps[1] = dariadb::storage::StepKind::MINUTE;
		steps[2] = dariadb::storage::StepKind::HOUR;
		ms.set_steps(steps);

		auto value = dariadb::Meas::empty(0);
		size_t writes_count = 10000;
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
		{//seconds
			dariadb::storage::QueryInterval qi({ 0 }, 0, 0, value.time);
			auto readed = ms.readInterval(qi);
			BOOST_CHECK_EQUAL(readed.size(), size_t(writes_count / 2)+1);
		}

		{//minutes
			dariadb::storage::QueryInterval qi({ 1 }, 0, 0, value.time);
			auto readed = ms.readInterval(qi);
			BOOST_CHECK_EQUAL(readed.size(), size_t(writes_count / (2*60)) + 1); //2*60
		}

		{//hour
			dariadb::storage::QueryInterval qi({ 2 }, 0, 0, value.time);
			auto readed = ms.readInterval(qi);
			BOOST_CHECK_EQUAL(readed.size(), size_t(writes_count / (2*60*60)) + 1);
		}

		{//minMax
			auto min = ms.minTime();
			BOOST_CHECK_EQUAL(min,500);
			auto max = ms.maxTime();
			BOOST_CHECK_EQUAL(max, value.time);

			auto res = ms.minMaxTime(0, &min, &max);
			BOOST_CHECK(res);
			BOOST_CHECK_EQUAL(min, 500);
			BOOST_CHECK_EQUAL(max, value.time);

			res = ms.minMaxTime(777, &min, &max);
			BOOST_CHECK(!res);
		}
	}
	dariadb::utils::async::ThreadManager::stop();
	if (dariadb::utils::fs::path_exists(storage_path)) {
		dariadb::utils::fs::rm(storage_path);
	}
}