#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include <timedb.h>

BOOST_AUTO_TEST_CASE(d_64) {
	// 10 0000 0000
    BOOST_CHECK_EQUAL(timedb::Compression::compress_delta_64(1), (uint16_t)257);
    BOOST_CHECK_EQUAL(timedb::Compression::compress_delta_64(64), (uint16_t)320);
    BOOST_CHECK_EQUAL(timedb::Compression::compress_delta_64(63), (uint16_t)319);
}

BOOST_AUTO_TEST_CASE(d_256) {
	// 1100 0000 0000
    BOOST_CHECK_EQUAL(timedb::Compression::compress_delta_256(256), (uint16_t)3328);
    BOOST_CHECK_EQUAL(timedb::Compression::compress_delta_256(255), (uint16_t)3327);
    BOOST_CHECK_EQUAL(timedb::Compression::compress_delta_256(65),  (uint16_t)3137);
}

BOOST_AUTO_TEST_CASE(d_2048) {
	// 1110 0000 0000 0000
    BOOST_CHECK_EQUAL(timedb::Compression::compress_delta_2048(2048), (uint16_t)59392);
    BOOST_CHECK_EQUAL(timedb::Compression::compress_delta_2048(257),  (uint16_t)57601);
    BOOST_CHECK_EQUAL(timedb::Compression::compress_delta_2048(4095), (uint16_t)61439);
}

BOOST_AUTO_TEST_CASE(d_big) {
	// 1111 0000 0000 0000 0000
	//      0000 0000 0000 0000
    BOOST_CHECK_EQUAL(timedb::Compression::compress_delta_big(2049),       (uint64_t)64424511489);
    BOOST_CHECK_EQUAL(timedb::Compression::compress_delta_big(65535),      (uint64_t)64424574975);
    BOOST_CHECK_EQUAL(timedb::Compression::compress_delta_big(4095),       (uint64_t)64424513535);
    BOOST_CHECK_EQUAL(timedb::Compression::compress_delta_big(4294967295), (uint64_t)68719476735);
}
