#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include <compression.h>

#include <iterator>
#include <sstream>

BOOST_AUTO_TEST_CASE(d_64) {
	BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_64(1), 513);
	BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_64(64), 576);
	BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_64(63), 575);
}

BOOST_AUTO_TEST_CASE(d_256) {
	BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_256(256), 3328);
	BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_256(255), 3327);
	BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_256(65), 3137);
}

BOOST_AUTO_TEST_CASE(d_2048) {
	BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_2048(2048), 59392);
	BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_2048(257), 57601);
	BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_2048(4095), 61439);
}

BOOST_AUTO_TEST_CASE(d_big) {
    BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_big(2049), uint64_t(64424511489));
    BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_big(65535),uint64_t( 64424574975));
    BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_big(4095), uint64_t(64424513535));
    BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_big(4294967295), uint64_t(68719476735));
}

BOOST_AUTO_TEST_CASE(binary_writer) {
	const size_t buffer_size = 10;
	const size_t writed_bits = 7 * buffer_size;
    uint8_t buffer[buffer_size];
	//check ctor
    timedb::compression::BinaryWriter b(std::begin(buffer),std::end(buffer));
	BOOST_CHECK_EQUAL(b.cap(), buffer_size);

	BOOST_CHECK_EQUAL(b.bitnum(), 7);
    BOOST_CHECK_EQUAL(b.pos(), size_t(0));
	
	//check incs work fine
	b.incbit();
	BOOST_CHECK_EQUAL(b.bitnum(), 6);
    BOOST_CHECK_EQUAL(b.pos(), size_t(0));

	b.incbit(); b.incbit(); b.incbit(); 
	b.incbit(); b.incbit(); b.incbit();
	b.incbit();
	BOOST_CHECK_EQUAL(b.bitnum(), 7);
    BOOST_CHECK_EQUAL(b.pos(), size_t(1));

	
	{//ctors test.
		timedb::compression::BinaryWriter copy_b(b);
		BOOST_CHECK_EQUAL(b.bitnum(), copy_b.bitnum());
		BOOST_CHECK_EQUAL(b.pos(), copy_b.pos());
		auto move_b = std::move(copy_b);
        BOOST_CHECK(size_t(copy_b.bitnum()) == copy_b.pos() && size_t(copy_b.bitnum()) == copy_b.cap());
        BOOST_CHECK_EQUAL(copy_b.bitnum(),0);

		BOOST_CHECK_EQUAL(move_b.bitnum(), b.bitnum());
		BOOST_CHECK_EQUAL(move_b.pos(), b.pos());
		BOOST_CHECK_EQUAL(move_b.cap(), b.cap());
	}
	// set/clr bit 
	b.reset_pos();
	// write 101010101...
	for (size_t i = 0; i <writed_bits; i++) {
		if (i % 2) {
			b.setbit();
		}
		else {
			b.clrbit();
		}
		b.incbit();
	}

	b.reset_pos();

	for (size_t i = 0; i < writed_bits; i++) {
		if (i % 2) {
			BOOST_CHECK_EQUAL(b.getbit(), uint8_t(1));
		}
		else {
			BOOST_CHECK_EQUAL(b.getbit(), uint8_t(0));
		}
		b.incbit();
	}

    std::stringstream ss{};
    ss<<b;
    BOOST_CHECK(ss.str().size()!=0);

	b.reset_pos();
	// clear all bits
	for (size_t i = 0; i < writed_bits; i++) {
		b.clrbit();
		BOOST_CHECK_EQUAL(b.getbit(), uint8_t(0));
		b.incbit();
	}
}
