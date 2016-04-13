#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include "test_common.h"

#include <union_storage.h>
#include <timeutil.h>
#include <utils/fs.h>
#include <utils/logger.h>

class BenchCallback:public dariadb::storage::ReaderClb{
public:
    void call(const dariadb::Meas&){
        count++;
    }
    size_t count;
};


BOOST_AUTO_TEST_CASE(UnionStorage) {
    const std::string storage_path = "testStorage";
    {// All values must be placed in the page. Without overwriting the old.
        const size_t chunk_per_storage = 10000;
        const size_t chunk_size = 256;
        const size_t cap_max_size = 100;
        const dariadb::Time write_window_deep = 500;
        const dariadb::Time old_mem_chunks=500;

        if (dariadb::utils::fs::path_exists(storage_path)) {
            dariadb::utils::fs::rm(storage_path);
        }

        dariadb::storage::BaseStorage_ptr ms{
            new dariadb::storage::UnionStorage(
			dariadb::storage::PageManager::Params(storage_path, dariadb::storage::MODE::SINGLE, chunk_per_storage, chunk_size),
				dariadb::storage::Capacitor::Params(cap_max_size,write_window_deep),
				dariadb::storage::UnionStorage::Limits(old_mem_chunks,0)) };

        auto e = dariadb::Meas::empty();
        //max time always
        dariadb::Time t = dariadb::timeutil::current_time();
        dariadb::Time start_time = t;
        size_t count = 0;
        for (size_t i = 0; ; i++) {
            count++;
            e.time = t;
            e.value++;
            t += 10;
            BOOST_CHECK(ms->append(e).writed==1);
            if (dariadb::storage::PageManager::instance()->chunks_in_cur_page()>0) {
                break;
            }
        }

        BOOST_CHECK(dariadb::utils::fs::ls(storage_path, ".page").size() == 1);

		dariadb::storage::ChuncksList all_chunks;
		ms->chunksByIterval(dariadb::IdArray{}, 0, start_time, t)->readAll(&all_chunks);
        auto min_time = std::numeric_limits<dariadb::Time>::max();
        auto max_time = std::numeric_limits<dariadb::Time>::min();
        for (auto c : all_chunks) {
            min_time = std::min(c->minTime, min_time);
            max_time = std::max(c->maxTime, max_time);
        }

        BOOST_CHECK(min_time == start_time);

        dariadb::Meas::MeasList output;
        ms->readInterval(start_time, t)->readAll(&output);
        BOOST_CHECK(output.size() <= count);

        std::shared_ptr<BenchCallback> clbk{ new BenchCallback };
        ms->readInterval(start_time, e.time)->readAll(clbk.get());
        BOOST_CHECK_GT(clbk->count,size_t(0));

        output.clear();
        ms->flush();
        ms->readInterval(start_time, t)->readAll(&output);
        BOOST_CHECK(output.size() >0);
        /*
		//TODO resolve it.
		dariadb::Value tst_val = 1;
        for (auto v : output) {
            BOOST_CHECK_EQUAL(v.value, tst_val);
            tst_val++;
        }*/


    }

    if (dariadb::utils::fs::path_exists(storage_path)) {
        dariadb::utils::fs::rm(storage_path);
    }
}

BOOST_AUTO_TEST_CASE(UnionStorage_common_test) {
	const std::string storage_path = "testStorage";
	const size_t chunk_per_storage = 10000;
	const size_t chunk_size = 1024;
	const size_t cap_max_size = 500;
	const dariadb::Time write_window_deep = 2000;
    const dariadb::Time whaitwrite_window_deep = 3000;

	const dariadb::Time from = dariadb::timeutil::current_time();
	const dariadb::Time to = from + 20000;
	const dariadb::Time step = 100;

	{
		if (dariadb::utils::fs::path_exists(storage_path)) {
			dariadb::utils::fs::rm(storage_path);
		}


		dariadb::storage::BaseStorage_ptr ms{
			new dariadb::storage::UnionStorage(
			dariadb::storage::PageManager::Params(storage_path, dariadb::storage::MODE::SINGLE, chunk_per_storage,chunk_size),
				dariadb::storage::Capacitor::Params(cap_max_size,write_window_deep),
				dariadb::storage::UnionStorage::Limits(0, 0)) };

	

        dariadb_test::storage_test_check(ms.get(), from, to, step, dariadb::Time(whaitwrite_window_deep));

		BOOST_CHECK(dariadb::utils::fs::path_exists(storage_path));
	}
	{
		dariadb::storage::BaseStorage_ptr ms{
			new dariadb::storage::UnionStorage(
			dariadb::storage::PageManager::Params(storage_path, dariadb::storage::MODE::SINGLE, chunk_per_storage, chunk_size),
				dariadb::storage::Capacitor::Params(cap_max_size,write_window_deep),
				dariadb::storage::UnionStorage::Limits(0,0)) };

		dariadb::Meas::MeasList mlist;
		ms->currentValue(dariadb::IdArray{},0)->readAll(&mlist);
		BOOST_CHECK(mlist.size() == size_t((to-from)/step));
	}
	if (dariadb::utils::fs::path_exists(storage_path)) {
		dariadb::utils::fs::rm(storage_path);
	}
}

BOOST_AUTO_TEST_CASE(UnionStorage_drop_chunks) {
    const std::string storage_path = "testStorage";
    const size_t chunk_per_storage = 1000;
    const size_t chunk_size = 100;
    const size_t cap_max_size = 10;
    const dariadb::Time write_window_deep = 1000;

    {
        if (dariadb::utils::fs::path_exists(storage_path)) {
            dariadb::utils::fs::rm(storage_path);
        }

        const size_t max_mem_chunks=5;
		auto raw_ptr = new dariadb::storage::UnionStorage(
			dariadb::storage::PageManager::Params(storage_path, dariadb::storage::MODE::SINGLE, chunk_per_storage,chunk_size),
			dariadb::storage::Capacitor::Params(cap_max_size, write_window_deep),
			dariadb::storage::UnionStorage::Limits(0, max_mem_chunks));
        
		dariadb::storage::BaseStorage_ptr ms{raw_ptr};

        auto m=dariadb::Meas::empty();
        m.id=1;
        m.time=dariadb::timeutil::current_time();
        for(size_t i=0;i<size_t(5000);i++){
            m.time+=10;
            m.value++;
            raw_ptr->append(m);
            auto val=raw_ptr->chunks_in_memory();
            BOOST_CHECK(val<=max_mem_chunks);
        }
        
    }
	if (dariadb::utils::fs::path_exists(storage_path)) {
		dariadb::utils::fs::rm(storage_path);
	}
}
