#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main

#include <boost/test/unit_test.hpp>
#include <libdariadb/meas.h>
#include <libdariadb/utils/radix.h>

BOOST_AUTO_TEST_CASE(TypeTraitsForMeasurement) {
	dariadb::utils::RadixPlusTree<dariadb::Time, dariadb::Meas> tree;
	BOOST_CHECK_EQUAL(tree.keys_count(), size_t(0));
}