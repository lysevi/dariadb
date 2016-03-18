#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <statistic.h>

class Moc_I1:public dariadb::statistic::BaseMethod {
public:
    Moc_I1(){
        _a=_b=dariadb::Meas::empty();
    }
    void calc(const dariadb::Meas&a,const dariadb::Meas&b)override{
        _a=a;
        _b=b;
    }
	dariadb::Value result()const {
		return dariadb::Value();
	}
    dariadb::Meas _a;
    dariadb::Meas _b;
};

BOOST_AUTO_TEST_CASE(CallCalc) {
    std::unique_ptr<Moc_I1>  p{new Moc_I1};
    auto m=dariadb::Meas::empty();
    m.id=1;
    p->call(m);
    BOOST_CHECK_EQUAL(p->_a.id,dariadb::Id(0));
    m.id=2;

    p->call(m);
    BOOST_CHECK_EQUAL(p->_a.id,dariadb::Id(1));
    BOOST_CHECK_EQUAL(p->_b.id,dariadb::Id(2));

    m.id=3;
    p->call(m);
    BOOST_CHECK_EQUAL(p->_a.id,dariadb::Id(2));
    BOOST_CHECK_EQUAL(p->_b.id,dariadb::Id(3));
}

BOOST_AUTO_TEST_CASE(RectangleMethod) {
	{//left
		using dariadb::statistic::integral::RectangleMethod;
		std::unique_ptr<RectangleMethod>  p{ new RectangleMethod(RectangleMethod::Kind::LEFT) };

		auto m = dariadb::Meas::empty();
		m.id = 1;

		m.time = 0;	m.value = 0;
		p->call(m);
		m.time = 1;	m.value = 1;
		p->call(m);
		BOOST_CHECK_EQUAL(p->result(), dariadb::Value(0));

		m.time = 2;	m.value = 2;
		p->call(m);
		BOOST_CHECK_EQUAL(p->result(), dariadb::Value(1));
	}

	{//right
		using dariadb::statistic::integral::RectangleMethod;
		std::unique_ptr<RectangleMethod>  p{ new RectangleMethod(RectangleMethod::Kind::RIGHT) };

		auto m = dariadb::Meas::empty();
		m.id = 1;

		m.time = 0;	m.value = 0;
		p->call(m);
		m.time = 1;	m.value = 1;
		p->call(m);
		BOOST_CHECK_EQUAL(p->result(), dariadb::Value(1));

		m.time = 2;	m.value = 2;
		p->call(m);
		BOOST_CHECK_EQUAL(p->result(), dariadb::Value(3));
	}


	{//midle
		using dariadb::statistic::integral::RectangleMethod;
		std::unique_ptr<RectangleMethod>  p{ new RectangleMethod(RectangleMethod::Kind::MIDLE) };

		auto m = dariadb::Meas::empty();
		m.id = 1;

		m.time = 0;	m.value = 0;
		p->call(m);
		m.time = 1;	m.value = 1;
		p->call(m);
		BOOST_CHECK_CLOSE(p->result(), dariadb::Value(0.5),0.01);

		m.time = 2;	m.value = 2;
		p->call(m);
		BOOST_CHECK_CLOSE(p->result(), dariadb::Value(2), 0.01);
	}
}
BOOST_AUTO_TEST_CASE(Average) {

	{//midle
		using dariadb::statistic::average::Average;
		std::unique_ptr<Average>  p{ new Average() };

		auto m = dariadb::Meas::empty();
		m.id = 1;

		m.time = 0;	m.value = 0;
		p->call(m);
		m.time = 1;	m.value = 1;
		p->call(m);

		m.time = 2;	m.value = 2;
		p->call(m);
		BOOST_CHECK_CLOSE(p->result(), dariadb::Value(1), 0.01);
	}
}