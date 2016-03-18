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
