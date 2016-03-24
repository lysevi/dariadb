#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <page_manager.h>

using dariadb::storage::PageManager;

BOOST_AUTO_TEST_CASE(PageManagerInstance) {
  PageManager::start();
  BOOST_CHECK(PageManager::instance()!=nullptr);
  PageManager::stop();
}
