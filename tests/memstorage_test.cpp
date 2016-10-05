#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main

#include <boost/test/unit_test.hpp>
#include <libdariadb/meas.h>
#include <libdariadb/ads/radix.h>
#include <libdariadb/ads/lockfree_array.h>

BOOST_AUTO_TEST_CASE(TypeTraitArrayLockFreeTest) {
	dariadb::ads::LockFreeArray<int> lf(10);
	BOOST_CHECK_EQUAL(lf.size(), size_t(10));

	dariadb::ads::LockFreeArray<int*> lfs(10);
	BOOST_CHECK_EQUAL(lfs.size(), size_t(10));
}


BOOST_AUTO_TEST_CASE(ArrayLockFreeTest) {
	{
		dariadb::ads::LockFreeArray<bool> lf(3);
		BOOST_CHECK_EQUAL(lf.size(), size_t(3));

		lf.store(0, true);
		lf.store(1, true);

		dariadb::ads::LockFreeArray<bool> lf_c(std::move(lf));
		BOOST_CHECK(lf_c[0]);
		BOOST_CHECK(lf_c[1]);
		BOOST_CHECK(!lf_c[2]);
	}
	{
		dariadb::ads::LockFreeArray<bool> lf(5);
		BOOST_CHECK_EQUAL(lf.cap(), lf.size());
		
		size_t i = 0;
		while (lf.insert(true)) {
			i++;
		}
		
		dariadb::ads::LockFreeArray<bool> midle(lf);
		dariadb::ads::LockFreeArray<bool> lf_c=midle;

		BOOST_CHECK_EQUAL(lf_c.size(), i);
		BOOST_CHECK_EQUAL(lf_c.cap(), size_t(0));
		for (size_t i = 0; i < lf_c.size(); ++i) {
			BOOST_CHECK(lf_c[i]);
		}
	}
}

BOOST_AUTO_TEST_CASE(TypeTraitsForMeasurement) {
	dariadb::ads::RadixPlusTree<dariadb::Time, dariadb::Meas> tree;
	BOOST_CHECK_EQUAL(tree.keys_count(), size_t(0));
}