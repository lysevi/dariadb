#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <cassert>

#include <time_ordered_set.h>
#include <timeutil.h>
#include <bucket.h>

class Moc_Storage :public memseries::storage::AbstractStorage {
public:
	size_t writed_count;
	std::vector<memseries::Meas> meases;
	memseries::append_result append(const memseries::Meas::PMeas begin, const size_t size) {
		writed_count+=size;
		return memseries::append_result(size,0);
	}
	memseries::append_result append(const memseries::Meas &value) {
		meases.push_back(value);
		writed_count += 1;
		return memseries::append_result(1,0);
	}
	memseries::storage::Reader_ptr readInterval(const memseries::IdArray &ids, memseries::Flag flag, memseries::Time from, memseries::Time to) {
		return nullptr;
	}

	memseries::storage::Reader_ptr readInTimePoint(const memseries::IdArray &ids, memseries::Flag flag, memseries::Time time_point) {
		return nullptr;
	}
	memseries::Time minTime() {
		return 0;
	}
	/// max time of writed meas
	memseries::Time maxTime() {
		return 0;
	}
};

BOOST_AUTO_TEST_CASE(TimeOrderedSetTest)
{
	const size_t max_size = 10;
	auto base = memseries::storage::TimeOrderedSet{ max_size };
	
	//with move ctor check
	memseries::storage::TimeOrderedSet tos(std::move(base));
	auto e = memseries::Meas::empty();
	for (size_t i = 0; i < max_size; i++) {
		e.id = i % 3;
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
	std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
	stor->writed_count = 0;
    const size_t max_size = 10;
    const memseries::Time write_window_deep = 1000;
    auto base = memseries::storage::Bucket{ max_size, stor,write_window_deep};

    //with move ctor check
    memseries::storage::Bucket mbucket(std::move(base));
    auto e = memseries::Meas::empty();

    //max time always
    memseries::Time t= memseries::timeutil::current_time();
    auto t_2=t+10;
	for (size_t i = 0; i < max_size; i++) {
        e.time = t_2;
        t_2 += 1;
		BOOST_CHECK(mbucket.append(e));
	}

    //past
    for (size_t i = 0; i < 5; i++) {
        e.time = t;
        t += 1;
        BOOST_CHECK(mbucket.append(e));
    }

    //cur time
    t= memseries::timeutil::current_time();
    for (size_t i = 0; i < max_size; i++) {
        e.time = t++;
        BOOST_CHECK(mbucket.append(e));
    }

	//buckets count;
    BOOST_CHECK(mbucket.size()>0);
    //now bucket is full
	//TODO remove this
    //BOOST_CHECK(!mbucket.append(e));

	//drop part of data to storage;
    //TODO uncomment that
//	e.time= memseries::timeutil::current_time()- write_window_deep*2;
//	BOOST_CHECK(!mbucket.append(e));

    //future
    t=memseries::timeutil::current_time();
    t+=20;
    for (size_t i = 0; i < max_size; i++) {
        e.time =t;
        t++;
        BOOST_CHECK(mbucket.append(e));
    }
    //BOOST_CHECK(mbucket.size()>100);

    //time should be increased
    //for (size_t i = 0; i < stor->meases.size() - 1; i++) {
    //	BOOST_CHECK(stor->meases[i].time<stor->meases[i+1].time);
    //}

    stor->meases.clear();
    stor->writed_count = 0;
    mbucket.flush();

    for (size_t i = 0; i < stor->meases.size() - 1; i++) {
        BOOST_CHECK(stor->meases[i].time<=stor->meases[i + 1].time);
        if(stor->meases[i].time>stor->meases[i + 1].time){
            assert(false);
        }
    }


    //check write data with time less than write_window_deep;
    stor->meases.clear();
    stor->writed_count = 0;
    t=memseries::timeutil::current_time()-write_window_deep;
    for (size_t i = 0; i < 100; i++) {
        e.time =t;
        t++;
        BOOST_CHECK(mbucket.append(e));
    }

    for (size_t i = 0; i < 10000; i++) {
        e.time =memseries::timeutil::current_time();
        BOOST_CHECK(mbucket.append(e));
    }
    auto first_size=stor->meases.size();
    BOOST_CHECK(first_size>size_t(0));
    stor->meases.clear();
    mbucket.flush();
    BOOST_CHECK(stor->meases.size()>=first_size);
}
