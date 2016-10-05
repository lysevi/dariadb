#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main

#include <boost/test/unit_test.hpp>
#include <libdariadb/meas.h>
#include <libdariadb/ads/radix.h>
#include <libdariadb/ads/lockfree_array.h>

BOOST_AUTO_TEST_CASE(TypeTraitArrayLockFreeTest) {
	dariadb::utils::LockFreeArray<int> lf(10);
	BOOST_CHECK_EQUAL(lf.size(), size_t(10));

	dariadb::utils::LockFreeArray<int*> lfs(10);
	BOOST_CHECK_EQUAL(lfs.size(), size_t(10));
}

BOOST_AUTO_TEST_CASE(TypeTraitsForMeasurement) {
	dariadb::utils::RadixPlusTree<dariadb::Time, dariadb::Meas> tree;
	BOOST_CHECK_EQUAL(tree.keys_count(), size_t(0));
}