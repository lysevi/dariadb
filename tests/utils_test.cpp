#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <utils.h>

BOOST_AUTO_TEST_CASE(UtilsEmpty) {
  BOOST_CHECK(dariadb::utils::inInterval(1, 5, 1));
  BOOST_CHECK(dariadb::utils::inInterval(1, 5, 2));
  BOOST_CHECK(dariadb::utils::inInterval(1, 5, 5));
  BOOST_CHECK(!dariadb::utils::inInterval(1, 5, 0));
  BOOST_CHECK(!dariadb::utils::inInterval(0, 1, 2));
}

BOOST_AUTO_TEST_CASE(BitOperations) {
    uint8_t value = 0;
	for (int8_t i = 0; i < 7; i++) {
        value=dariadb::utils::BitOperations::set(value, i);
		BOOST_CHECK_EQUAL(dariadb::utils::BitOperations::check(value, i), true);
	}

	for (int8_t i = 0; i < 7; i++) {
        value=dariadb::utils::BitOperations::clr(value, i);
	}

	for (int8_t i = 0; i < 7; i++) {
		BOOST_CHECK_EQUAL(dariadb::utils::BitOperations::check(value, i), false);
	}
}
