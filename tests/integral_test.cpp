#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <integral.h>

class Moc_I1:public memseries::statistic::BaseIntegral{
public:
    Moc_I1(){
        _a=_b=memseries::Meas::empty();
    }
    void calc(const memseries::Meas&a,const memseries::Meas&b)override{
        _a=a;
        _b=b;
    }
	memseries::Value result()const {
		return memseries::Value();
	}
    memseries::Meas _a;
    memseries::Meas _b;
};

BOOST_AUTO_TEST_CASE(CallCalc) {
    std::unique_ptr<Moc_I1>  p{new Moc_I1};
    auto m=memseries::Meas::empty();
    m.id=1;
    p->call(m);
    BOOST_CHECK_EQUAL(p->_a.id,memseries::Id(0));
    m.id=2;

    p->call(m);
    BOOST_CHECK_EQUAL(p->_a.id,memseries::Id(1));
    BOOST_CHECK_EQUAL(p->_b.id,memseries::Id(2));

    m.id=3;
    p->call(m);
    BOOST_CHECK_EQUAL(p->_a.id,memseries::Id(2));
    BOOST_CHECK_EQUAL(p->_b.id,memseries::Id(3));
}

BOOST_AUTO_TEST_CASE(RectangleMethod) {
	{//left
		using memseries::statistic::RectangleMethod;
		std::unique_ptr<RectangleMethod>  p{ new RectangleMethod(RectangleMethod::Kind::LEFT) };

		auto m = memseries::Meas::empty();
		m.id = 1;

		m.time = 0;	m.value = 0;
		p->call(m);
		m.time = 1;	m.value = 1;
		p->call(m);
		BOOST_CHECK_EQUAL(p->result(), memseries::Value(0));

		m.time = 2;	m.value = 2;
		p->call(m);
		BOOST_CHECK_EQUAL(p->result(), memseries::Value(1));
	}

	{//right
		using memseries::statistic::RectangleMethod;
		std::unique_ptr<RectangleMethod>  p{ new RectangleMethod(RectangleMethod::Kind::RIGHT) };

		auto m = memseries::Meas::empty();
		m.id = 1;

		m.time = 0;	m.value = 0;
		p->call(m);
		m.time = 1;	m.value = 1;
		p->call(m);
		BOOST_CHECK_EQUAL(p->result(), memseries::Value(1));

		m.time = 2;	m.value = 2;
		p->call(m);
		BOOST_CHECK_EQUAL(p->result(), memseries::Value(3));
	}


	{//midle
		using memseries::statistic::RectangleMethod;
		std::unique_ptr<RectangleMethod>  p{ new RectangleMethod(RectangleMethod::Kind::MIDLE) };

		auto m = memseries::Meas::empty();
		m.id = 1;

		m.time = 0;	m.value = 0;
		p->call(m);
		m.time = 1;	m.value = 1;
		p->call(m);
		BOOST_CHECK_CLOSE(p->result(), memseries::Value(0.5),0.01);

		m.time = 2;	m.value = 2;
		p->call(m);
		BOOST_CHECK_CLOSE(p->result(), memseries::Value(2), 0.01);
	}
}