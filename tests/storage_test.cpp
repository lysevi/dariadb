#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include <meas.h>
#include <storage.h>
#include <string>

void checkAll(timedb::Meas::MeasList res, std::string msg, timedb::Time from,timedb::Time to,timedb::Time  step) {
    for (auto i = from; i < to; i += step) {
        bool is_error=true;
        for(auto &m:res) {
            if ((m.id == i) || (m.flag == i) || (m.time == i)) {
                is_error=false;
                break;
            }
            if (is_error) {
                BOOST_MESSAGE(msg);
            }
        }
    }
}

void storage_test_check(timedb::storage::AbstractStorage *as,timedb::Time from,timedb::Time to,timedb::Time  step){
    auto m = timedb::Meas::empty();
    size_t total_count = 0;
    for (auto i = from; i < to; i += step) {
        m.id = i;
        m.flag = i;
        m.time = i;
        BOOST_CHECK(as->append(m).writed==1);
		total_count++;
		m.time++;
		BOOST_CHECK(as->append(m).writed == 1);
        total_count++;
    }

    timedb::Meas::MeasList all{};
    as->readInterval(from,to)->readAll(&all);
    BOOST_CHECK_EQUAL(all.size(), total_count);

    checkAll(all, "readAll error: ",from,to,step);

    timedb::IdArray ids{};
    all.clear();
    as->readInterval(ids,0,from,to)->readAll(&all);
    BOOST_CHECK_EQUAL(all.size(), total_count);

    checkAll(all, "read error: ",from,to,step);

    ids.push_back(from+step);
    timedb::Meas::MeasList fltr_res{};
    as->readInterval(ids,0,from,to)->readAll(&fltr_res);

    BOOST_CHECK_EQUAL(fltr_res.size(), size_t(2));

    BOOST_CHECK_EQUAL(fltr_res.front().id,ids[0]);

    fltr_res.clear();
    as->readInterval(ids,to+1,from,to)->readAll(&fltr_res);
    BOOST_CHECK_EQUAL(fltr_res.size(),size_t(0));

    all.clear();
    as->readInTimePoint(to)->readAll(&all);
    BOOST_CHECK_EQUAL(all.size(),total_count);

    checkAll(all, "TimePoint error: ",from,to,step);


    timedb::IdArray emptyIDs{};
    fltr_res.clear();
    as->readInTimePoint(to)->readAll(&fltr_res);
    BOOST_CHECK_EQUAL(fltr_res.size(),total_count);

    checkAll(all, "TimePointFltr error: ",from,to,step);

    auto magicFlag = timedb::Flag(from + step);
    fltr_res.clear();
    as->readInTimePoint(emptyIDs,magicFlag,to)->readAll(&fltr_res);
    BOOST_CHECK_EQUAL(fltr_res.size(),size_t(2));

    BOOST_CHECK_EQUAL(fltr_res.front().flag,magicFlag);
}


BOOST_AUTO_TEST_CASE(inFilter) {
	{
		BOOST_CHECK(timedb::in_filter(0, 100));
		BOOST_CHECK(!timedb::in_filter(1, 100));
	}
}

BOOST_AUTO_TEST_CASE(MemoryStorage) {
	{
		auto ms = new timedb::storage::MemoryStorage{ 1000 };
		storage_test_check(ms, 0, 1000, 2);
		delete ms;
	}
}

