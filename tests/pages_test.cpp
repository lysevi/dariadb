#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <page_manager.h>
#include <compression.h>

using dariadb::storage::PageManager;

BOOST_AUTO_TEST_CASE(PageManagerInstance) {
  PageManager::start(dariadb::storage::STORAGE_MODE::SINGLE,1,1);
  BOOST_CHECK(PageManager::instance()!=nullptr);
  PageManager::stop();
}


BOOST_AUTO_TEST_CASE(PageManagerReadWrite) {
	const size_t chunks_count = 10;
	const size_t chunks_size = 100;

    PageManager::start(dariadb::storage::STORAGE_MODE::SINGLE,chunks_count, chunks_size);
	BOOST_CHECK(PageManager::instance() != nullptr);
	
	auto t= dariadb::Time(0);

	for (size_t cur_chunk_num = 0; cur_chunk_num < chunks_count; cur_chunk_num++) {
		dariadb::Meas first;
		first.id = 1;
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
	}

	//must return all of appended chunks;
	auto all_chunks=PageManager::instance()->get_chunks(dariadb::IdArray{}, 0, t, 0);
	auto readed_t = dariadb::Time(0);
	
	BOOST_CHECK_EQUAL(all_chunks.size(), size_t(chunks_count));
	for (auto ch : all_chunks) {
		BOOST_CHECK(ch->is_readonly);
		
		ch->bw->reset_pos();
		dariadb::compression::CopmressedReader crr(ch->bw, ch->first);
		for (uint32_t i = 0; i < ch->count; i++) {
			auto m = crr.read();
			BOOST_CHECK_EQUAL(m.time, readed_t);
			readed_t++;
		}
	}

	PageManager::stop();
}
