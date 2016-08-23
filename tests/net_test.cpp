#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <net/client.h>
#include <net/server.h>
#include <utils/locker.h>

const dariadb::net::Server::Param server_param(2001);
const dariadb::net::Client::Param client_param("127.0.0.1", 2001);

BOOST_AUTO_TEST_CASE(Instance) {
    dariadb::utils::Locker lock;
    std::lock_guard<dariadb::utils::Locker> lg(lock);
  dariadb::net::Server s(server_param);
  BOOST_CHECK_EQUAL(s.connections_accepted(),size_t(0));

  dariadb::net::Client c(client_param);
  c.connect();

  while(s.connections_accepted()!=size_t(1)){

  }
}
