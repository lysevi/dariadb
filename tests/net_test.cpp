#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <net/client.h>
#include <net/server.h>

BOOST_AUTO_TEST_CASE(Instance) {
  dariadb::net::Server *s=new dariadb::net::Server();
  dariadb::net::Client *c=new dariadb::net::Client();
  delete s;
  delete c;
}
