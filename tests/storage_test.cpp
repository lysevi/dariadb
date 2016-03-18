#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include <meas.h>
#include <memstorage.h>
#include <time_ordered_set.h>
#include <flags.h>
#include <string>
#include <thread>
#include <iostream>

const size_t copies_count = 100;

void checkAll(dariadb::Meas::MeasList res,
              std::string msg,
              dariadb::Time from,
              dariadb::Time to, dariadb::Time  step) {
	for (auto i = from; i < to; i += step) {
		size_t count = 0;
		for (auto &m : res) {
			if ((m.id == i) 
				&& ((m.flag == i) ||(m.flag==dariadb::Flags::NO_DATA)) 
				&& (m.time == i)) {
				count++;
			}

		}
		if (count < copies_count) {
			BOOST_CHECK_EQUAL(copies_count, count);
		}
	}
}

void check_reader_of_all(dariadb::storage::Reader_ptr reader,
	dariadb::Time from,
	dariadb::Time to,
	dariadb::Time  step, 
	size_t total_count,std::string message)
{
	dariadb::Meas::MeasList all{};
	reader->readAll(&all);
	BOOST_CHECK_EQUAL(all.size(), total_count);
	auto readed_ids = reader->getIds();
	BOOST_CHECK_EQUAL(readed_ids.size(), size_t(to / step));

	checkAll(all, message, from, to, step);
}

void storage_test_check(dariadb::storage::AbstractStorage *as,
                        dariadb::Time from,
                        dariadb::Time to,
                        dariadb::Time  step) {
	auto m = dariadb::Meas::empty();
	size_t total_count = 0;

	for (auto i = from; i < to; i += step) {
		m.id = i;
		m.flag = dariadb::Flag(i);
		m.time = i;
		m.value = 0;
		for (size_t j = 1; j < copies_count+1; j++) {
			BOOST_CHECK(as->append(m).writed == 1);
			total_count++;
			m.value = j;
		}
	}

	
	auto reader = as->readInterval(from, to);
	check_reader_of_all(reader, from, to, step, total_count, "readAll error: ");

	auto cloned_reader=reader->clone();
	cloned_reader->reset();
	check_reader_of_all(cloned_reader, from, to, step, total_count, "cloned readAll error: ");

	dariadb::IdArray ids{};
	dariadb::Meas::MeasList all{};
	as->readInterval(ids, 0, from, to)->readAll(&all);
    BOOST_CHECK_EQUAL(all.size(), total_count);

	checkAll(all, "read error: ", from, to, step);

    ids.push_back(from + step);
	dariadb::Meas::MeasList fltr_res{};
    as->readInterval(ids, 0, from, to)->readAll(&fltr_res);

    BOOST_CHECK_EQUAL(fltr_res.size(), copies_count);

	all.clear();
	as->readInTimePoint(to)->readAll(&all);
	size_t ids_count = (size_t)((to - from) / step);
	BOOST_CHECK_EQUAL(all.size(), ids_count);

	dariadb::IdArray emptyIDs{};
	fltr_res.clear();
	as->readInTimePoint(to)->readAll(&fltr_res);
	BOOST_CHECK_EQUAL(fltr_res.size(), ids_count);

    dariadb::IdArray notExstsIDs{9999};
    fltr_res.clear();
    as->readInTimePoint(notExstsIDs,0,to-1)->readAll(&fltr_res);
    BOOST_CHECK_EQUAL(fltr_res.size(), size_t(1));
    BOOST_CHECK_EQUAL(fltr_res.front().flag, dariadb::Flags::NO_DATA);

}


BOOST_AUTO_TEST_CASE(inFilter) {
	{
		BOOST_CHECK(dariadb::in_filter(0, 100));
		BOOST_CHECK(!dariadb::in_filter(1, 100));
	}
}

BOOST_AUTO_TEST_CASE(MemoryStorage) {
	{
		auto ms = new dariadb::storage::MemoryStorage{ 500 };
		const dariadb::Time from = 0;
		const dariadb::Time to = 100;
		const dariadb::Time step = 2;
		storage_test_check(ms, from, to, step);
		BOOST_CHECK_EQUAL(ms->chunks_size(), (to - from) / step); // id per chunk.
		delete ms;
	}
}

void thread_writer(dariadb::Id id,
                   dariadb::Time from,
                   dariadb::Time to,
                   dariadb::Time step,
                   dariadb::storage::MemoryStorage *ms)
{
	auto m = dariadb::Meas::empty();
	for (auto i = from; i < to; i += step) {
		m.id = id;
		m.flag = dariadb::Flag(i);
		m.time = i;
		m.value = dariadb::Value(i);
		ms->append(m);
	}
}


void thread_reader(dariadb::Id id,
	dariadb::Time from,
	dariadb::Time to,
	size_t expected,
	dariadb::storage::MemoryStorage *ms)
{
	dariadb::IdArray ids;
	if (id != 0) {
		ids.push_back(id);
	}

	if (to == 0) {
		auto rdr = ms->readInTimePoint(ids, 0, from);
		dariadb::Meas::MeasList out;
		rdr->readAll(&out);
		BOOST_CHECK_EQUAL(out.size(), expected);
	}
	else {
		auto rdr = ms->readInterval(ids, 0, from, to);
		dariadb::Meas::MeasList out;
		rdr->readAll(&out);
		BOOST_CHECK_EQUAL(out.size(), expected);
	}
}

BOOST_AUTO_TEST_CASE(MultiThread)
{
	auto ms = new dariadb::storage::MemoryStorage{ 500 };
	std::thread t1(thread_writer, 0, 0, 100, 2, ms);
	std::thread t2(thread_writer, 1, 0, 100, 2, ms);
	std::thread t3(thread_writer, 2, 0, 100, 2, ms);
	std::thread t4(thread_writer, 0, 0, 100, 1, ms);

	t1.join();
	t2.join();
	t3.join();
	t4.join();

    std::thread rt1(thread_reader, 0, 0, 100, (50+50+50+100),  ms);
	std::thread rt2(thread_reader, 1, 50, 0,  1, ms);
	std::thread rt3(thread_reader, 2, 0, 100, 50,ms);
	std::thread rt4(thread_reader, 0, 50, 0, 3, ms);

	rt1.join();
	rt2.join();
	rt3.join();
	rt4.join();

	delete ms;
}

BOOST_AUTO_TEST_CASE(ReadInterval)
{
	auto ds = new dariadb::storage::MemoryStorage{ 500 };
	dariadb::Meas m;
	{
		m.id = 1; m.time = 1;
		ds->append(m);
		m.id = 2; m.time = 2;
		ds->append(m);

		m.id = 4; m.time = 4;
		ds->append(m);
		m.id = 5; m.time = 5;
		ds->append(m);
		m.id = 55; m.time = 5;
		ds->append(m);

		{
			auto tp_reader = ds->readInTimePoint(6);
			dariadb::Meas::MeasList output_in_point{};
			tp_reader->readAll(&output_in_point);

			BOOST_CHECK_EQUAL(output_in_point.size(), size_t(5));

			auto rdr = ds->readInterval(0, 6);
			output_in_point.clear();
			rdr->readAll(&output_in_point);
			BOOST_CHECK_EQUAL(output_in_point.size(), size_t(5));
		}
		{

			auto tp_reader = ds->readInTimePoint(3);
			dariadb::Meas::MeasList output_in_point{};
			tp_reader->readAll(&output_in_point);

			BOOST_CHECK_EQUAL(output_in_point.size(), size_t(2+3));//+ timepoimt(3) with no_data
			for (auto v : output_in_point) {
				BOOST_CHECK(v.time <= 3);
			}
		}
		auto reader = ds->readInterval(3, 5);
		dariadb::Meas::MeasList output{};
		reader->readAll(&output);
		BOOST_CHECK_EQUAL(output.size(), size_t(5+3));//+ timepoimt(3) with no_data
	}
	// from this point read not from firsts.
	{
		m.id = 1; m.time = 6;
		ds->append(m);
		m.id = 2; m.time = 7;
		ds->append(m);

		m.id = 4; m.time = 9;
		ds->append(m);
		m.id = 5; m.time = 10;
		ds->append(m);
        m.id = 6; m.time = 10;
		ds->append(m);
		{

			auto tp_reader = ds->readInTimePoint(8);
			dariadb::Meas::MeasList output_in_point{};
			tp_reader->readAll(&output_in_point);

			BOOST_CHECK_EQUAL(output_in_point.size(), size_t(5+1));//+ timepoimt(8) with no_data
			for (auto v : output_in_point) {
				BOOST_CHECK(v.time <= 8);
			}
		}

        auto reader = ds->readInterval(dariadb::IdArray{ 1,2,4,5,55 }, 0, 8, 10);
        dariadb::Meas::MeasList output{};
        reader->readAll(&output);
        BOOST_CHECK_EQUAL(output.size(), size_t(7));

        if(output.size()!=size_t(7)){
            std::cout<<" ERROR!!!!"<<std::endl;
            //reader->readNext(nullptr);
            for(dariadb::Meas v:output){
                std::cout<<" id:"<<v.id
                        <<" flg:"<<v.flag
                       <<" v:"<<v.value
                      <<" t:"<<v.time<<std::endl;
            }
        }


	}
	delete ds;
}


BOOST_AUTO_TEST_CASE(byStep) {
	const size_t id_count = 1;
	{// equal step
		auto ms = new dariadb::storage::MemoryStorage{ 500 };
		
		auto m = dariadb::Meas::empty();
		const size_t total_count = 100;
		const dariadb::Time time_step = 1;
		
		for (size_t i = 0; i < total_count; i += time_step) {
			m.id = i%id_count;
			m.flag = dariadb::Flag(i);
			m.time = i;
			m.value = 0;
			ms->append(m);
		}
		auto rdr=ms->readInterval(0, total_count);

		dariadb::Meas::MeasList allByStep;
		rdr = ms->readInterval(0, total_count);
		rdr->readByStep(&allByStep,0,total_count, time_step);
		auto expected = size_t(total_count / time_step)*id_count;//+ timepoint
		BOOST_CHECK_EQUAL(allByStep.size(), expected);
		delete ms;
	}

	{// less step
		auto ms = new dariadb::storage::MemoryStorage{ 500 };

		auto m = dariadb::Meas::empty();
		const size_t total_count = 100;
		const dariadb::Time time_step = 10;
		
		for (size_t i = 0; i < total_count; i += time_step) {
			m.id = i%id_count;
			m.flag = dariadb::Flag(i);
			m.time = i;
			m.value = 0;
			ms->append(m);
		}
		
		auto rdr = ms->readInterval(0, total_count);

		dariadb::Time query_step = 11;
		dariadb::Meas::MeasList allByStep;
		rdr = ms->readInterval(0, total_count);
		rdr->readByStep(&allByStep, 0, total_count, query_step);
		auto expected = size_t(total_count / query_step)*id_count + id_count;//+ timepoint;
		BOOST_CHECK_EQUAL(allByStep.size(), expected); 
		delete ms;
	}

	{// great step
		auto ms = new dariadb::storage::MemoryStorage{ 500 };

		auto m = dariadb::Meas::empty();
		const size_t total_count = 100;
		const dariadb::Time time_step = 10;

		for (size_t i = 0; i < total_count; i += time_step) {
			m.id = i%id_count;
			m.flag = dariadb::Flag(i);
			m.time = i;
			m.value = 0;
			ms->append(m);
		}

		auto rdr = ms->readInterval(0, total_count);
		dariadb::Meas::MeasList all;
		rdr->readAll(&all);

		dariadb::Time query_step = 5;
		dariadb::Meas::MeasList allByStep;
		rdr = ms->readInterval(0, total_count);
		rdr->readByStep(&allByStep, 0, total_count, query_step);
        auto expected = size_t(total_count / time_step)*2 * id_count + id_count;//+ timepoint;
		BOOST_CHECK_EQUAL(allByStep.size(), expected);
		delete ms;
	}

	{// from before data
		auto ms = new dariadb::storage::MemoryStorage{ 500 };

		auto m = dariadb::Meas::empty();
		const size_t total_count = 100;
		const dariadb::Time time_step = 10;

		for (size_t i = time_step; i < total_count; i += time_step) {
			m.id = i%id_count;
			m.flag = dariadb::Flag(i);
			m.time = i;
			m.value = 0;
			ms->append(m);
		}

		auto rdr = ms->readInterval(time_step, total_count);
		dariadb::Meas::MeasList all;
		rdr->readAll(&all);

		dariadb::Time query_step = 5;
		dariadb::Meas::MeasList allByStep;
		rdr = ms->readInterval(time_step, total_count);

		rdr->readByStep(&allByStep, 0, total_count, query_step);
		
		dariadb::Time expected = dariadb::Time((total_count - time_step) / time_step) * 2;
		expected= expected* id_count;
		expected += id_count*(time_step / query_step);//+ before first value
		expected += id_count;//one after last  value

		BOOST_CHECK_EQUAL(allByStep.size(), expected);
		delete ms;
	}
}

class Moc_SubscribeClbk : public dariadb::storage::ReaderClb {
public:
    std::list<dariadb::Meas> values;
	void call(const dariadb::Meas&m) override {
        values.push_back(m);
	}
	~Moc_SubscribeClbk() {}
};

BOOST_AUTO_TEST_CASE(Subscribe) {
	const size_t id_count = 5;
	std::shared_ptr<Moc_SubscribeClbk> c1(new Moc_SubscribeClbk);
	std::shared_ptr<Moc_SubscribeClbk> c2(new Moc_SubscribeClbk);
	std::shared_ptr<Moc_SubscribeClbk> c3(new Moc_SubscribeClbk);
	std::shared_ptr<Moc_SubscribeClbk> c4(new Moc_SubscribeClbk);

	std::unique_ptr<dariadb::storage::MemoryStorage> ms{ new dariadb::storage::MemoryStorage{ 500 } };
	dariadb::IdArray ids{};
    ms->subscribe(ids, 0, c1); //all
    ids.push_back(2);
    ms->subscribe(ids, 0, c2); // 2
    ids.push_back(1);
    ms->subscribe(ids, 0, c3); // 1 2
    ids.clear();
    ms->subscribe(ids, 1, c4); // with flag=1

	auto m = dariadb::Meas::empty();
	const size_t total_count = 100;
	const dariadb::Time time_step = 1;

	for (size_t i = 0; i < total_count; i += time_step) {
		m.id = i%id_count;
		m.flag = dariadb::Flag(i);
		m.time = i;
		m.value = 0;
		ms->append(m);
	}
    BOOST_CHECK_EQUAL(c1->values.size(),total_count);

    BOOST_CHECK_EQUAL(c2->values.size(),size_t(total_count/id_count));
    BOOST_CHECK_EQUAL(c2->values.front().id,dariadb::Id(2));

    BOOST_CHECK_EQUAL(c3->values.size(),size_t(total_count/id_count)*2);
    BOOST_CHECK_EQUAL(c3->values.front().id,dariadb::Id(1));

    BOOST_CHECK_EQUAL(c4->values.size(),size_t(1));
    BOOST_CHECK_EQUAL(c4->values.front().flag,dariadb::Flag(1));
}
