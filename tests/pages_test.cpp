#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <algorithm>

#include <storage/page_manager.h>
#include <compression.h>
#include <utils/fs.h>
#include <utils/utils.h>
#include <storage/page.h>

using dariadb::storage::PageManager;

BOOST_AUTO_TEST_CASE(PageManagerInstance) {
	const std::string storagePath = "testStorage";
  PageManager::start(PageManager::Params(storagePath,dariadb::storage::MODE::SINGLE,1,1));
  BOOST_CHECK(PageManager::instance()!=nullptr);
  PageManager::stop();
}

dariadb::Time add_chunk(dariadb::Id id, dariadb::Time t, size_t chunks_size){
    dariadb::Meas first;
    first.id = id;
    first.time = t;
    dariadb::storage::Chunk_Ptr ch = std::make_shared<dariadb::storage::Chunk>(chunks_size, first);

    for (int i = 0;; i++,t++) {
        first.flag = dariadb::Flag(i);
        first.time = t;
        first.value = dariadb::Value(i);
        if (!ch->append(first)) {
            break;
        }
    }

    auto res = PageManager::instance()->append_chunk(ch);
    BOOST_CHECK(res);
    return t;
}

BOOST_AUTO_TEST_CASE(PageManagerReadWrite) {
	const std::string storagePath = "testStorage/";
	const size_t chunks_count = 10;
	const size_t chunks_size = 100;

	if (dariadb::utils::fs::path_exists(storagePath)) {
		dariadb::utils::fs::rm(storagePath);
	}

    PageManager::start(PageManager::Params(storagePath,dariadb::storage::MODE::SINGLE,chunks_count, chunks_size));
	BOOST_CHECK(PageManager::instance() != nullptr);
	
	auto start_time = dariadb::Time(0);
	auto t= dariadb::Time(0);

	dariadb::Id id(0);
	const dariadb::Id id_count(2);
	for (size_t cur_chunk_num = 0; cur_chunk_num < chunks_count; cur_chunk_num++) {
        t=add_chunk(id%id_count,t,chunks_size);
		id++;
    }
	
    dariadb::Time minTime(t);
    {// Chunks load
        //must return all of appended chunks;
        dariadb::storage::ChuncksList all_chunks;
        PageManager::instance()->chunksByIterval(dariadb::IdArray{}, dariadb::Flag(0), 0, t)->readAll(&all_chunks);
        auto readed_t = dariadb::Time(0);

        BOOST_CHECK_EQUAL(all_chunks.size(), size_t(chunks_count));
        for (auto ch : all_chunks) {
            //BOOST_CHECK(ch->is_readonly);

            minTime=std::min(minTime,ch->minTime);
            ch->bw->reset_pos();
            dariadb::compression::CopmressedReader crr(ch->bw, ch->first);
            for (uint32_t i = 0; i < ch->count; i++) {
                auto m = crr.read();
                BOOST_CHECK_EQUAL(m.time, readed_t);
                readed_t++;
            }
        }

		{
			dariadb::Time end_time (t / 2);
			dariadb::storage::ChuncksList chunk_list;
			PageManager::instance()->chunksByIterval(dariadb::IdArray{}, dariadb::Flag(0), start_time, end_time)->readAll(&chunk_list);
			BOOST_CHECK(chunk_list.size() == size_t((chunks_count / 2)+1));
			
			for (auto&v : chunk_list) {
				BOOST_CHECK(v->minTime <= end_time);
			}
			
			auto chunks_map = PageManager::instance()->chunksBeforeTimePoint(dariadb::IdArray{}, 0, end_time);
			BOOST_CHECK(chunks_map.size() == size_t(id_count));

			for (auto&kv : chunks_map) {
				auto chunk = kv.second;
				auto is_in_interval = dariadb::utils::inInterval(chunk->minTime, chunk->maxTime, end_time);
				BOOST_CHECK(is_in_interval || chunk->maxTime<end_time);
			}

			auto ids_array=PageManager::instance()->getIds();
			BOOST_CHECK_EQUAL(ids_array.size(), size_t(2));
			BOOST_CHECK(std::find(ids_array.begin(), ids_array.end(), dariadb::Id(0)) != ids_array.end());
			BOOST_CHECK(std::find(ids_array.begin(), ids_array.end(), dariadb::Id(1)) != ids_array.end());
		}

    }
	BOOST_CHECK(dariadb::utils::fs::path_exists(storagePath));
	BOOST_CHECK(dariadb::utils::fs::ls(storagePath).size()==1);
	
    {//rewrite oldes chunk
        dariadb::Time minTime_replaced(t);
        t=add_chunk(dariadb::Id(1), t,chunks_size);

		auto cursor = PageManager::instance()->chunksByIterval(dariadb::IdArray{}, dariadb::Flag(0),0, t);
        dariadb::storage::ChuncksList all_chunks;
        cursor->readAll(&all_chunks);
        BOOST_CHECK_EQUAL(all_chunks.size(), size_t(chunks_count));

        for (dariadb::storage::Chunk_Ptr ch : all_chunks) {
            minTime_replaced=std::min(minTime_replaced,ch->minTime);
        }

        BOOST_CHECK(minTime_replaced>minTime);

		//reset_pos test.
		cursor->reset_pos();
        all_chunks.clear();
        cursor->readAll(&all_chunks);
		BOOST_CHECK_EQUAL(all_chunks.size(), size_t(chunks_count));
    }
	PageManager::stop();

	if (dariadb::utils::fs::path_exists(storagePath)) {
		dariadb::utils::fs::rm(storagePath);
	}
}

BOOST_AUTO_TEST_CASE(PageManagerReadWriteWithContinue) {
	const std::string storagePath = "testStorage";
	const size_t chunks_count = 10;
	const size_t chunks_size = 200;
	auto t = dariadb::Time(0);

	if (dariadb::utils::fs::path_exists(storagePath)) {
		dariadb::utils::fs::rm(storagePath);
	}

    PageManager::start(PageManager::Params(storagePath,dariadb::storage::MODE::SINGLE, chunks_count, chunks_size));
	dariadb::Meas first;
	first.id = 1;
	first.time = t;
	{
		dariadb::storage::Chunk_Ptr ch = std::make_shared<dariadb::storage::Chunk>(chunks_size, first);

		for (size_t i = 0; i < (chunks_size / 10); i++, t++) {
			first.flag = dariadb::Flag(i);
			first.time = t;
			first.value = dariadb::Value(i);
			if (!ch->append(first)) {
				assert(false);
			}
		}
		auto res = PageManager::instance()->append_chunk(ch);
		BOOST_CHECK(res);
	}
	PageManager::stop();

	auto header=dariadb::storage::Page::readHeader(dariadb::utils::fs::append_path(storagePath, "single.page"));
	BOOST_CHECK_EQUAL(header.chunk_per_storage, chunks_count);
	BOOST_CHECK_EQUAL(header.chunk_size, chunks_size);
    BOOST_CHECK_EQUAL(header.count_readers, size_t(0));

    PageManager::start(PageManager::Params(storagePath, dariadb::storage::MODE::SINGLE, chunks_count, chunks_size));

    dariadb::storage::ChuncksList all_chunks;
	all_chunks=PageManager::instance()->get_open_chunks();
    BOOST_CHECK_EQUAL(all_chunks.size(), size_t(1));
	if (all_chunks.size() != 0) {
		auto c = all_chunks.front();
        first.time ++;
		first.flag++;
		first.value++;
		BOOST_CHECK(c->append(first));
		
		c->bw->reset_pos();
		dariadb::compression::CopmressedReader crr(c->bw, c->first);

		for (uint32_t i = 0; i < c->count; i++) {
			auto m = crr.read();

            BOOST_CHECK_EQUAL(m.time, dariadb::Time(i));
			BOOST_CHECK_EQUAL(m.flag, dariadb::Flag(i));
			BOOST_CHECK_EQUAL(m.value, dariadb::Value(i));
            BOOST_CHECK_EQUAL(m.id, dariadb::Id(1));
		}

	}
	PageManager::stop();

	if (dariadb::utils::fs::path_exists(storagePath)) {
		dariadb::utils::fs::rm(storagePath);
	}
}
