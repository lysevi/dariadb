#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include <storage.h>

BOOST_AUTO_TEST_CASE(MemoryStorage) {
    auto ms=new timedb::storage::MemoryStorage(100);
}

