#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <union_storage.h>
#include <timeutil.h>
#include <utils/fs.h>

BOOST_AUTO_TEST_CASE(UnionStorage) {
    const std::string storage_path = "testStorage";
	{
        const size_t chunk_per_storage = 5000;
        const size_t chunk_size = 100;
        const size_t cap_max_size = 100;
        const dariadb::Time write_window_deep = 500;
        const dariadb::Time old_mem_chunks=500;

		if (dariadb::utils::fs::path_exists(storage_path)) {
			dariadb::utils::fs::rm(storage_path);
		}

		dariadb::storage::BaseStorage_ptr ms{
            new dariadb::storage::UnionStorage(storage_path,
			dariadb::storage::STORAGE_MODE::SINGLE,
				chunk_per_storage,
				chunk_size,
				write_window_deep,
                cap_max_size,old_mem_chunks) };

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
			if (dariadb::utils::fs::path_exists(storage_path)) {
				break;
			}
		}

		BOOST_CHECK(dariadb::utils::fs::ls(storage_path, ".page").size() == 1);
		
		auto all_chunks=ms->chunksByIterval(dariadb::IdArray{}, 0, start_time, t);
		auto min_time = std::numeric_limits<dariadb::Time>::max();
		auto max_time = std::numeric_limits<dariadb::Time>::min();
		for (auto c : all_chunks) {
			min_time = std::min(c->minTime, min_time);
			max_time = std::max(c->maxTime, max_time);
		}
		
		BOOST_CHECK(min_time == start_time);

		dariadb::Meas::MeasList output;
		ms->readInterval(start_time, t)->readAll(&output);
		BOOST_CHECK(output.size() < count);

		output.clear();
		ms->flush();
		ms->readInterval(start_time, t)->readAll(&output);
		BOOST_CHECK(output.size() == count);
		dariadb::Value tst_val = 1;
		for (auto v : output) {
			BOOST_CHECK_EQUAL(v.value, tst_val);
			tst_val++;
		}

		
	}

    if (dariadb::utils::fs::path_exists(storage_path)) {
        dariadb::utils::fs::rm(storage_path);
    }
}

