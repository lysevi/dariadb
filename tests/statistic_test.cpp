#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <math/statistic.h>
#include <storage/memstorage.h>

BOOST_AUTO_TEST_CASE(RectangleMethod) {
  { // left
    using dariadb::statistic::integral::RectangleMethod;
    std::unique_ptr<RectangleMethod> p{new RectangleMethod(RectangleMethod::Kind::LEFT)};

    auto m = dariadb::Meas::empty();
    m.id = 1;

    m.time = 0;
    m.value = 0;
    p->call(m);
    m.time = 1;
    m.value = 1;
    p->call(m);
    BOOST_CHECK_EQUAL(p->result(), dariadb::Value(0));

    m.time = 2;
    m.value = 2;
    p->call(m);
    BOOST_CHECK_EQUAL(p->result(), dariadb::Value(1));
  }

  { // right
    using dariadb::statistic::integral::RectangleMethod;
    std::unique_ptr<RectangleMethod> p{new RectangleMethod(RectangleMethod::Kind::RIGHT)};

    auto m = dariadb::Meas::empty();
    m.id = 1;

    m.time = 0;
    m.value = 0;
    p->call(m);
    m.time = 1;
    m.value = 1;
    p->call(m);
    BOOST_CHECK_EQUAL(p->result(), dariadb::Value(1));

    m.time = 2;
    m.value = 2;
    p->call(m);
    BOOST_CHECK_EQUAL(p->result(), dariadb::Value(3));
  }

  { // midle
    using dariadb::statistic::integral::RectangleMethod;
    std::unique_ptr<RectangleMethod> p{new RectangleMethod(RectangleMethod::Kind::MIDLE)};

    auto m = dariadb::Meas::empty();
    m.id = 1;

    m.time = 0;
    m.value = 0;
    p->call(m);
    m.time = 1;
    m.value = 1;
    p->call(m);
    BOOST_CHECK_CLOSE(p->result(), dariadb::Value(0.5), 0.01);

    m.time = 2;
    m.value = 2;
    p->call(m);
    BOOST_CHECK_CLOSE(p->result(), dariadb::Value(2), 0.01);
  }
}
BOOST_AUTO_TEST_CASE(Average) {
  { // midle
    using dariadb::statistic::average::Average;
    std::unique_ptr<Average> p{new Average()};

    auto m = dariadb::Meas::empty();
    m.id = 1;

    m.time = 0;
    m.value = 0;
    p->call(m);
    m.time = 1;
    m.value = 1;
    p->call(m);

    m.time = 2;
    m.value = 2;
    p->call(m);
    BOOST_CHECK_CLOSE(p->result(), dariadb::Value(1), 0.01);
  }
}
