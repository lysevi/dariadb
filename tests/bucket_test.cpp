#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include <time_ordered_set.h>
#include <bucket.h>

BOOST_AUTO_TEST_CASE(TimeOrderedSetTest)
{
	const size_t max_size = 10;
	auto base = memseries::storage::TimeOrderedSet{ max_size };
	
	//with move ctor check
	memseries::storage::TimeOrderedSet tos(std::move(base));
	auto e = memseries::Meas::empty();
	for (size_t i = 0; i < max_size; i++) {
		e.time = max_size - i;
		BOOST_CHECK(!tos.is_full());
		BOOST_CHECK(tos.append(e));
    }

	e.time = max_size;
	BOOST_CHECK(!tos.append(e)); //is_full
	BOOST_CHECK(tos.is_full());

	{//check copy ctor and operator=
		memseries::storage::TimeOrderedSet copy_tos{ tos };
		BOOST_CHECK_EQUAL(copy_tos.size(), max_size);

		memseries::storage::TimeOrderedSet copy_assign{};
		copy_assign = copy_tos;

		auto a = copy_assign.as_array();
		BOOST_CHECK_EQUAL(a.size(), max_size);
		for (size_t i = 1; i <= max_size; i++) {
			auto e = a[i - 1];
			BOOST_CHECK_EQUAL(e.time, i);
		}
		BOOST_CHECK_EQUAL(copy_assign.minTime(), memseries::Time(1));
		BOOST_CHECK_EQUAL(copy_assign.maxTime(), memseries::Time(max_size));
	}
    BOOST_CHECK(tos.is_full());
    e.time=max_size+1;
    BOOST_CHECK(tos.append(e,true));
    BOOST_CHECK_EQUAL(tos.size(),max_size+1);
}

BOOST_AUTO_TEST_CASE(BucketTest)
{
    const size_t max_size = 10;
    const size_t max_count = 10;
    auto base = memseries::storage::Bucket{ max_size, max_count};

    //with move ctor check
    memseries::storage::Bucket mbucket(std::move(base));
    BOOST_CHECK_EQUAL(mbucket.max_size(),max_count);
    auto e = memseries::Meas::empty();
    //max time always
    memseries::Time t=10;
    for (size_t j = 0; j < max_count; j++){
        for (size_t i = 0; i < max_size; i++) {
            e.time = t;
            t+=10;
            BOOST_CHECK(mbucket.append(e));
        }
    }
    BOOST_CHECK_EQUAL(mbucket.size(),max_count);
    //now bucket is full
    //BOOST_CHECK(!mbucket.append(e));

    //insert in midle buckets
    e.time = 12;
    BOOST_CHECK(mbucket.append(e));
    e.time = 13;
    BOOST_CHECK(mbucket.append(e));
    e.time = 14;
    BOOST_CHECK(mbucket.append(e));
    e.time = max_size+1;
    BOOST_CHECK(mbucket.append(e));
    e.time=1;
    // insert between {from,to} V {min,max}
    for (size_t j = 0; j < max_count; j++){
        for (size_t i = 0; i < max_size; i++) {
            e.time +=1;
            BOOST_CHECK(mbucket.append(e));
        }
    }
}
