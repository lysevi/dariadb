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
