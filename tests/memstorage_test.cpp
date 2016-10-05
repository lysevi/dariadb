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


BOOST_AUTO_TEST_CASE(ArrayLockFreeTest) {
	{
		dariadb::utils::LockFreeArray<bool> lf(3);
		BOOST_CHECK_EQUAL(lf.size(), size_t(3));

		lf.store(0, true);
		lf.store(1, true);

		dariadb::utils::LockFreeArray<bool> lf_c(std::move(lf));
		BOOST_CHECK(lf_c[0]);
		BOOST_CHECK(lf_c[1]);
		BOOST_CHECK(!lf_c[2]);
	}
	{
		dariadb::utils::LockFreeArray<bool> lf(5);
		BOOST_CHECK_EQUAL(lf.cap(), lf.size());
		
		size_t i = 0;
		while (lf.insert(true)) {
			i++;
		}
		BOOST_CHECK_EQUAL(lf.size(), i);
		BOOST_CHECK_EQUAL(lf.cap(), size_t(0));
		for (size_t i = 0; i < lf.size(); ++i) {
			BOOST_CHECK(lf[i]);
		}
	}
}

BOOST_AUTO_TEST_CASE(TypeTraitsForMeasurement) {
	dariadb::utils::RadixPlusTree<dariadb::Time, dariadb::Meas> tree;
	BOOST_CHECK_EQUAL(tree.keys_count(), size_t(0));
}