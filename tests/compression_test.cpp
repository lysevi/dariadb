#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include <timedb.h>

BOOST_AUTO_TEST_CASE(d_64) {
	// 10 0000 0000
	BOOST_CHECK_EQUAL(timedb::Compression::compress_delta_64(1), 257);
	BOOST_CHECK_EQUAL(timedb::Compression::compress_delta_64(64), 320);
	BOOST_CHECK_EQUAL(timedb::Compression::compress_delta_64(63), 319);
}

BOOST_AUTO_TEST_CASE(d_256) {
	// 110 0000 0000
	BOOST_CHECK_EQUAL(timedb::Compression::compress_delta_256(256), 3328);
	BOOST_CHECK_EQUAL(timedb::Compression::compress_delta_256(255), 3327);
	BOOST_CHECK_EQUAL(timedb::Compression::compress_delta_256(65), 3137);
}