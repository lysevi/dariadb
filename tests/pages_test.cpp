#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <page_manager.h>

using dariadb::storage::PageManager;

BOOST_AUTO_TEST_CASE(PageManagerInstance) {
  PageManager::start(1,1);
  BOOST_CHECK(PageManager::instance()!=nullptr);
  PageManager::stop();
}


BOOST_AUTO_TEST_CASE(PageManagerAppendChunk) {
	const size_t chunks_count = 10;
	const size_t chunks_size = 100;

	PageManager::start(chunks_count, chunks_size);
	BOOST_CHECK(PageManager::instance() != nullptr);
	
	for (size_t cur_chunk_num = 0; cur_chunk_num < chunks_count; cur_chunk_num++) {
		dariadb::Meas first;
		first.id = 1;
		dariadb::storage::Chunk_Ptr ch = std::make_shared<dariadb::storage::Chunk>(chunks_size, first);

		for (int i = 0;; i++) {
			first.flag = dariadb::Flag(i);
			first.time = dariadb::Time(i);
			first.value = dariadb::Value(i);
			if (!ch->append(first)) {
				break;
			}
		}
		auto res = PageManager::instance()->append_chunk(ch);
		BOOST_CHECK(res);
	}

	PageManager::stop();
}