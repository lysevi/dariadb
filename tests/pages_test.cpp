#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <page_manager.h>
#include <compression.h>
#include <utils/fs.h>
#include <storage/page.h>

using dariadb::storage::PageManager;

BOOST_AUTO_TEST_CASE(PageManagerInstance) {
	const std::string storagePath = "testStorage";
  PageManager::start(storagePath,dariadb::storage::STORAGE_MODE::SINGLE,1,1);
  BOOST_CHECK(PageManager::instance()!=nullptr);
  PageManager::stop();
}

dariadb::Time add_chunk(dariadb::Time t, size_t chunks_size){
    dariadb::Meas first;
    first.id = 1;
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

    PageManager::start(storagePath,dariadb::storage::STORAGE_MODE::SINGLE,chunks_count, chunks_size);
	BOOST_CHECK(PageManager::instance() != nullptr);
	
	auto t= dariadb::Time(0);

	for (size_t cur_chunk_num = 0; cur_chunk_num < chunks_count; cur_chunk_num++) {
        t=add_chunk(t,chunks_size);
    }
    dariadb::Time minTime(t);
    {
        //must return all of appended chunks;
        dariadb::storage::ChuncksList all_chunks;
        PageManager::instance()->get_chunks(dariadb::IdArray{}, 0, t, 0)->readAll(&all_chunks);
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
    }
	BOOST_CHECK(dariadb::utils::fs::path_exists(storagePath));
	BOOST_CHECK(dariadb::utils::fs::ls(storagePath).size()==1);

    {//rewrite oldes chunk
        dariadb::Time minTime_replaced(t);
        t=add_chunk(t,chunks_size);

		auto cursor = PageManager::instance()->get_chunks(dariadb::IdArray{}, 0, t, 0);
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

	PageManager::start(storagePath,dariadb::storage::STORAGE_MODE::SINGLE, chunks_count, chunks_size);
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
	BOOST_CHECK_EQUAL(header.count_readers, 0);

	PageManager::start(storagePath, dariadb::storage::STORAGE_MODE::SINGLE, chunks_count, chunks_size);

    dariadb::storage::ChuncksList all_chunks;
    PageManager::instance()->get_chunks(dariadb::IdArray{}, 0, t, 0)->readAll(&all_chunks);
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
