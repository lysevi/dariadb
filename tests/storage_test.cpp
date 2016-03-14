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

void checkAll(memseries::Meas::MeasList res,
              std::string msg,
              memseries::Time from,
              memseries::Time to, memseries::Time  step) {
	for (auto i = from; i < to; i += step) {
		size_t count = 0;
		for (auto &m : res) {
			if ((m.id == i) && (m.flag == i) && (m.time == i)) {
				count++;
			}

		}
		if (count < copies_count) {
			BOOST_CHECK_EQUAL(copies_count, count);
		}
	}
}

void storage_test_check(memseries::storage::AbstractStorage *as,
                        memseries::Time from,
                        memseries::Time to,
                        memseries::Time  step) {
	auto m = memseries::Meas::empty();
	size_t total_count = 0;

	for (auto i = from; i < to; i += step) {
		m.id = i;
		m.flag = memseries::Flag(i);
		m.time = i;
		m.value = 0;
		for (size_t j = 1; j < copies_count+1; j++) {
			BOOST_CHECK(as->append(m).writed == 1);
			total_count++;
			m.value = j;
		}
	}

	memseries::Meas::MeasList all{};
	as->readInterval(from, to)->readAll(&all);
    BOOST_CHECK_EQUAL(all.size(), total_count);

	checkAll(all, "readAll error: ", from, to, step);

	memseries::IdArray ids{};
	all.clear();
	as->readInterval(ids, 0, from, to)->readAll(&all);
    BOOST_CHECK_EQUAL(all.size(), total_count);

	checkAll(all, "read error: ", from, to, step);

	ids.push_back(from + step);
	memseries::Meas::MeasList fltr_res{};
	as->readInterval(ids, 0, from, to)->readAll(&fltr_res);

	BOOST_CHECK_EQUAL(fltr_res.size(), copies_count);

	BOOST_CHECK_EQUAL(fltr_res.front().id, ids[0]);

	fltr_res.clear();
	as->readInterval(ids, memseries::Flag(to + 1), from, to)->readAll(&fltr_res);
	BOOST_CHECK_EQUAL(fltr_res.size(), size_t(0));

	all.clear();
	as->readInTimePoint(to)->readAll(&all);
	size_t ids_count = (size_t)((to - from) / step);
	BOOST_CHECK_EQUAL(all.size(), ids_count);

	memseries::IdArray emptyIDs{};
	fltr_res.clear();
	as->readInTimePoint(to)->readAll(&fltr_res);
	BOOST_CHECK_EQUAL(fltr_res.size(), ids_count);
}


BOOST_AUTO_TEST_CASE(inFilter) {
	{
		BOOST_CHECK(memseries::in_filter(0, 100));
		BOOST_CHECK(!memseries::in_filter(1, 100));
	}
}

BOOST_AUTO_TEST_CASE(MemoryStorage) {
	{
		auto ms = new memseries::storage::MemoryStorage{ 500 };
		const memseries::Time from = 0;
		const memseries::Time to = 100;
		const memseries::Time step = 2;
		storage_test_check(ms, from, to, step);
		BOOST_CHECK_EQUAL(ms->chunks_size(), (to - from) / step); // id per chunk.
		delete ms;
	}
}

void thread_writer(memseries::Id id,
                   memseries::Time from,
                   memseries::Time to,
                   memseries::Time step,
                   memseries::storage::MemoryStorage *ms)
{
	auto m = memseries::Meas::empty();
	for (auto i = from; i < to; i += step) {
		m.id = id;
		m.flag = memseries::Flag(i);
		m.time = i;
		m.value = 0;
		for (size_t j = 0; j < copies_count; j++) {
			ms->append(m);
			m.value = j;
		}
	}
}

//TODO uncomment this.
//BOOST_AUTO_TEST_CASE(MultiThread)
//{
//	auto ms = new memseries::storage::MemoryStorage{ 500 };
//	std::thread t1(thread_writer, 0, 0, 100, 2, ms);
//	std::thread t2(thread_writer, 1, 0, 100, 2, ms);
//	std::thread t3(thread_writer, 2, 0, 100, 2, ms);
//	std::thread t4(thread_writer, 3, 0, 100, 2, ms);

//	t1.join();
//	t2.join();
//	t3.join();
//	t4.join();
//	delete ms;
//}

BOOST_AUTO_TEST_CASE(ReadInterval)
{
	auto ds = new memseries::storage::MemoryStorage{ 500 };
	memseries::Meas m;
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
			memseries::Meas::MeasList output_in_point{};
			tp_reader->readAll(&output_in_point);

			BOOST_CHECK_EQUAL(output_in_point.size(), size_t(5));

			auto rdr = ds->readInterval(0, 6);
			output_in_point.clear();
			rdr->readAll(&output_in_point);
			BOOST_CHECK_EQUAL(output_in_point.size(), size_t(5));
		}
		{

			auto tp_reader = ds->readInTimePoint(3);
			memseries::Meas::MeasList output_in_point{};
			tp_reader->readAll(&output_in_point);

			BOOST_CHECK_EQUAL(output_in_point.size(), size_t(2));
			for (auto v : output_in_point) {
				BOOST_CHECK(v.time <= 3);
			}
		}
		auto reader = ds->readInterval(3, 5);
		memseries::Meas::MeasList output{};
		reader->readAll(&output);
		BOOST_CHECK_EQUAL(output.size(), size_t(5));
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
			memseries::Meas::MeasList output_in_point{};
			tp_reader->readAll(&output_in_point);

			BOOST_CHECK_EQUAL(output_in_point.size(), size_t(5));
			for (auto v : output_in_point) {
				BOOST_CHECK(v.time <= 8);
			}
		}

        auto reader = ds->readInterval(memseries::IdArray{ 1,2,4,5,55 }, 0, 8, 10);
        memseries::Meas::MeasList output{};
        reader->readAll(&output);
        BOOST_CHECK_EQUAL(output.size(), size_t(7));

        if(output.size()!=size_t(7)){
            std::cout<<" ERROR!!!!"<<std::endl;
            //reader->readNext(nullptr);
            for(memseries::Meas v:output){
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
	{// equal step
		auto ms = new memseries::storage::MemoryStorage{ 500 };
		
		auto m = memseries::Meas::empty();
		const size_t total_count = 100;
		const memseries::Time time_step = 10;

		for (size_t i = 0; i < total_count; i += time_step) {
			m.id = i;
			m.flag = memseries::Flag(i);
			m.time = i;
			m.value = 0;
			ms->append(m);
		}
		auto rdr=ms->readInterval(0, total_count);

		memseries::Meas::MeasList allByStep;
		rdr = ms->readInterval(0, total_count);
		rdr->readByStep(&allByStep, time_step);
		BOOST_CHECK_EQUAL(allByStep.size(), size_t(total_count/time_step));
		delete ms;
	}

	{// less step
		auto ms = new memseries::storage::MemoryStorage{ 500 };

		auto m = memseries::Meas::empty();
		const size_t total_count = 100;
		const memseries::Time time_step = 10;

		for (size_t i = 0; i < total_count; i += time_step) {
			m.id = i;
			m.flag = memseries::Flag(i);
			m.time = i;
			m.value = 0;
			ms->append(m);
		}
		
		auto rdr = ms->readInterval(0, total_count);

		memseries::Time query_step = 11;
		memseries::Meas::MeasList allByStep;
		rdr = ms->readInterval(0, total_count);
		rdr->readByStep(&allByStep, query_step);
		auto expected = size_t(total_count / query_step);
		BOOST_CHECK_EQUAL(allByStep.size(), expected);
		delete ms;
	}
}