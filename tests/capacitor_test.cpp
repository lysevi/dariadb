#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <cassert>
#include <thread>
#include <atomic>
#include <map>

#include <storage/time_ordered_set.h>
#include <timeutil.h>
#include <storage/capacitor.h>
#include <utils/logger.h>

class Moc_Storage :public dariadb::storage::BaseStorage {
public:
	size_t writed_count;
    std::map<dariadb::Id, std::vector<dariadb::Meas>> meases;
	dariadb::append_result append(const dariadb::Meas::PMeas , const size_t size) {
		writed_count+=size;
		return dariadb::append_result(size,0);
	}
	dariadb::append_result append(const dariadb::Meas &value) {
        meases[value.id].push_back(value);
		writed_count += 1;
		return dariadb::append_result(1,0);
	}

	dariadb::Time minTime() {
		return 0;
	}
	/// max time of writed meas
	dariadb::Time maxTime() {
		return 0;
	}

    void subscribe(const dariadb::IdArray&,const dariadb::Flag& , const dariadb::storage::ReaderClb_ptr &) override{
	}

	dariadb::storage::Reader_ptr currentValue(const dariadb::IdArray&, const dariadb::Flag&) override{
		return nullptr;
	}
	void flush()override {
	}
	dariadb::storage::Cursor_ptr chunksByIterval(const dariadb::IdArray &, dariadb::Flag, dariadb::Time, dariadb::Time) {
		return nullptr;
	}

	dariadb::storage::IdToChunkMap chunksBeforeTimePoint(const dariadb::IdArray &, dariadb::Flag, dariadb::Time) {
		return dariadb::storage::IdToChunkMap{};
	}
	dariadb::IdArray getIds() { return dariadb::IdArray{}; }
};

BOOST_AUTO_TEST_CASE(TimeOrderedSetTest)
{
	const size_t max_size = 10;
	auto base = dariadb::storage::TimeOrderedSet{ max_size };
	
	//with move ctor check
	dariadb::storage::TimeOrderedSet tos(std::move(base));
	auto e = dariadb::Meas::empty();
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
		dariadb::storage::TimeOrderedSet copy_tos{ tos };
		BOOST_CHECK_EQUAL(copy_tos.size(), max_size);

		dariadb::storage::TimeOrderedSet copy_assign{};
		copy_assign = copy_tos;

		auto a = copy_assign.as_array();
		BOOST_CHECK_EQUAL(a.size(), max_size);
		for (size_t i = 1; i <= max_size; i++) {
			e = a[i - 1];
			BOOST_CHECK_EQUAL(e.time, i);
		}
		BOOST_CHECK_EQUAL(copy_assign.minTime(), dariadb::Time(1));
		BOOST_CHECK_EQUAL(copy_assign.maxTime(), dariadb::Time(max_size));
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
    const dariadb::Time write_window_deep = 1000;
    

    //with move ctor check
    dariadb::storage::Capacitor mbucket(stor,dariadb::storage::Capacitor::Params(max_size,write_window_deep));
    auto e = dariadb::Meas::empty();

    //max time always
    dariadb::Time t= dariadb::timeutil::current_time();
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
    t= t_2;
    for (size_t i = 0; i < 5; i++) {
        e.time = t++;
        BOOST_CHECK(mbucket.append(e));
    }

    //buckets count;
    BOOST_CHECK(mbucket.size()>0);

	e.time= dariadb::timeutil::current_time()- write_window_deep*2;
	BOOST_CHECK(!mbucket.append(e));

    //future
    t=dariadb::timeutil::current_time();
    t+=20;
    for (size_t i = 0; i < max_size; i++) {
        e.time =t;
        t++;
        BOOST_CHECK(mbucket.append(e));
    }

    stor->meases.clear();
    stor->writed_count = 0;
    mbucket.flush();

    //time should be increased
    for(auto kv:stor->meases){
        for (size_t i = 0; i < kv.second.size() - 1; i++) {
            BOOST_CHECK(kv.second[i].time<=kv.second[i + 1].time);
            if(kv.second[i].time>kv.second[i + 1].time){
                logger("i: "<<i<<" lhs: "<<kv.second[i].time<<" rhs: "<<kv.second[i+1].time);
                assert(false);
            }
        }
    }


    //check write data with time less than write_window_deep;
    stor->meases.clear();
    stor->writed_count = 0;
    t=dariadb::timeutil::current_time()-write_window_deep;
    for (size_t i = 0; i < 100; i++) {
        e.time =t;
        t++;
        BOOST_CHECK(mbucket.append(e));
    }

	stor->meases.clear();
    while(stor->meases.size()==0){
        e.time =dariadb::timeutil::current_time();
        BOOST_CHECK(mbucket.append(e));
    }
}

std::atomic_long append_count{ 0 };

void thread_writer(dariadb::Id id,
	dariadb::Time from,
	dariadb::Time to,
	dariadb::Time step,
	dariadb::storage::Capacitor *cp)
{
    const size_t copies_count = 1;
	auto m = dariadb::Meas::empty();
    m.time = dariadb::timeutil::current_time();
	for (auto i = from; i < to; i += step) {
		m.id = id;
		m.flag = dariadb::Flag(i);
		m.value = 0;
		for (size_t j = 0; j < copies_count; j++) {
            m.time++;
            m.value = dariadb::Value(j);
			cp->append(m);
            append_count++;
		}
	}
}

BOOST_AUTO_TEST_CASE(MultiThread)
{
	std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
	stor->writed_count = 0;
	const size_t max_size = 10;
    const dariadb::Time write_window_deep = 10000;
	dariadb::storage::Capacitor mbucket{stor,dariadb::storage::Capacitor::Params(max_size,write_window_deep)};

    std::thread t1(thread_writer, 0, 0, 10, 1, &mbucket);
    std::thread t2(thread_writer, 1, 0, 10, 1, &mbucket);
    std::thread t3(thread_writer, 2, 0, 100, 2, &mbucket);
    std::thread t4(thread_writer, 3, 0, 100, 1, &mbucket);

	t1.join();
    t2.join();
    t3.join();
    t4.join();
	
	mbucket.flush();
    //BOOST_CHECK_EQUAL(stor->meases.size(), size_t(append_count.load()));
    size_t cnt=0;
    for(auto kv:stor->meases){
        cnt+=kv.second.size();
        for (size_t i = 0; i < kv.second.size() - 1; i++) {
            BOOST_CHECK(kv.second[i].time<=kv.second[i + 1].time);
            if(kv.second[i].time>kv.second[i + 1].time){
                logger("i: "<<i<<" lhs: "<<kv.second[i].time<<" rhs: "<<kv.second[i+1].time);
                assert(false);
            }
        }
    }
    BOOST_CHECK_EQUAL(cnt, size_t(append_count.load()));
}
