#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <utils.h>
#include <storage/fs.h>
#include <bloom_filter.h>

BOOST_AUTO_TEST_CASE(InInterval) {
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


BOOST_AUTO_TEST_CASE(BloomTest) {
	typedef uint8_t u8_fltr_t;

	auto u8_fltr = dariadb::bloom_empty<u8_fltr_t>();

	BOOST_CHECK_EQUAL(u8_fltr, uint8_t{ 0 });

	u8_fltr = dariadb::bloom_add(u8_fltr, uint8_t{ 1 });
	u8_fltr = dariadb::bloom_add(u8_fltr, uint8_t{ 2 });

	BOOST_CHECK(dariadb::bloom_check(u8_fltr, uint8_t{ 1 }));
	BOOST_CHECK(dariadb::bloom_check(u8_fltr, uint8_t{ 2 }));
	BOOST_CHECK(dariadb::bloom_check(u8_fltr, uint8_t{ 3 }));
	BOOST_CHECK(!dariadb::bloom_check(u8_fltr, uint8_t{ 4 }));
	BOOST_CHECK(!dariadb::bloom_check(u8_fltr, uint8_t{ 5 }));
}


BOOST_AUTO_TEST_CASE(FileUtils) {
  std::string filename = "foo/bar/test.txt";
  BOOST_CHECK_EQUAL(dariadb::storage::fs::filename(filename), "test");
  BOOST_CHECK_EQUAL(dariadb::storage::fs::parent_path(filename), "foo/bar");

  auto res=dariadb::storage::fs::ls(".");
  BOOST_CHECK(res.size()>0);
}
