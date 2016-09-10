#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main

#include <boost/asio/steady_timer.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <iostream>
#include <thread>
#include <atomic>

#include <meas.h>
#include <net/client.h>
#include <net/server.h>
#include <utils/logger.h>
#include <interfaces/imeasstorage.h>
#include <engine.h>

const dariadb::net::Server::Param server_param(2001);
const dariadb::net::client::Client::Param client_param("127.0.0.1", 2001);

std::atomic_bool server_runned;
std::atomic_bool server_stop_flag;
dariadb::net::Server *server_instance = nullptr;

void server_thread_func() {
  dariadb::net::Server s(server_param);

  BOOST_CHECK(!s.is_runned());

  s.start();

  while (!s.is_runned()) {
  }

  server_instance = &s;
  server_runned.store(true);
  while (!server_stop_flag.load()) {
  }

  server_instance = nullptr;
}

BOOST_AUTO_TEST_CASE(Connect1) {
  dariadb::logger("********** Connect1 **********");
  server_runned.store(false);
  server_stop_flag = false;
  std::thread server_thread{server_thread_func};

  while (!server_runned.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  dariadb::net::client::Client c(client_param);
  c.connect();

  // 1 client
  while (true) {
    auto res = server_instance->connections_accepted();
    auto st = c.state();
    dariadb::logger("Connect1 test>> ", "0 count: ", res, " state: ", st);
    if (res == size_t(1) && st == dariadb::net::ClientState::WORK) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  server_stop_flag = true;
  server_thread.join();
  while (true) {
    auto state = c.state();
    dariadb::logger("Connect1 test>> ", "4 state: ", state);
    if (state == dariadb::net::ClientState::DISCONNECTED) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
}

BOOST_AUTO_TEST_CASE(Connect3) {
  dariadb::logger("********** Connect3 **********");
  server_runned.store(false);
  server_stop_flag = false;
  std::thread server_thread{server_thread_func};

  while (!server_runned.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  dariadb::net::client::Client c(client_param);
  c.connect();

  // 1 client
  while (true) {
    auto res = server_instance->connections_accepted();
    auto st = c.state();
    dariadb::logger("Connect3 test>> ", "0 count: ", res, " state: ", st);
    if (res == size_t(1) && st == dariadb::net::ClientState::WORK) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  { // this check disconnect on client::dtor.
    dariadb::net::client::Client c2(client_param);
    c2.connect();

    // 2 client: c and c2
    while (true) {
      auto res = server_instance->connections_accepted();
      auto st = c2.state();
      dariadb::logger("Connect3 test>> ", "1 count: ", res, " state: ", st);
      if (res == size_t(2) && st == dariadb::net::ClientState::WORK) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }
  }

  // 1 client: c
  while (true) {
    auto res = server_instance->connections_accepted();
    std::cout << "Connect3 one expect: " << res << std::endl;
    if (res == size_t(1)) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  dariadb::net::client::Client c3(client_param);
  c3.connect();

  // 2 client: c and c3
  while (true) {
    auto res = server_instance->connections_accepted();
    auto st = c3.state();
    dariadb::logger("Connect3 test>> ", "2 count: ", res, " state: ", st);
    if (res == size_t(2) && st == dariadb::net::ClientState::WORK) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  c.disconnect();

  // 1 client: c3
  while (true) {
    auto res = server_instance->connections_accepted();
    auto st = c.state();
    dariadb::logger("Connect3 test>> ", "3 count: ", res, " state: ", st);
    if (res == size_t(1) && c.state() == dariadb::net::ClientState::DISCONNECTED) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
  server_stop_flag = true;
  server_thread.join();
  while (true) {
    auto state = c3.state();
    dariadb::logger("Connect3 test>> ", "4 state: ", state);
    if (state == dariadb::net::ClientState::DISCONNECTED) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
}

BOOST_AUTO_TEST_CASE(PingTest) {
  dariadb::logger("********** PingTest **********");
  const size_t MAX_PONGS = 2;
  server_runned.store(false);
  server_stop_flag = false;
  std::thread server_thread{server_thread_func};

  while (!server_runned.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  dariadb::net::client::Client c1(client_param);
  c1.connect();
  dariadb::net::client::Client c2(client_param);
  c2.connect();

  // 2 clients
  while (true) {
    auto st1 = c1.state();
    auto st2 = c2.state();
    dariadb::logger("PingTest test>> ", "0  state1: ", st1, " state2: ", st2);
    if (st1 == dariadb::net::ClientState::WORK &&
        st2 == dariadb::net::ClientState::WORK) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  while (true) {
    auto p1 = c1.pings_answers();
    auto p2 = c2.pings_answers();
    dariadb::logger("PingTest test>> p1:", p1, " p2:", p2);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    if (p1 > MAX_PONGS && p2 > MAX_PONGS) {
      break;
    }
  }
  c1.disconnect();
  c2.disconnect();

  while (true) {
    auto st1 = c1.state();
    auto st2 = c2.state();
    dariadb::logger("PingTest test>> ", "0  state1: ", st1, " state2: ", st2);
    if (st1 == dariadb::net::ClientState::DISCONNECTED &&
        st2 == dariadb::net::ClientState::DISCONNECTED) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  server_stop_flag = true;
  server_thread.join();
}

BOOST_AUTO_TEST_CASE(ReadWriteTest) {
    dariadb::logger("********** ReadWriteTest **********");

  const std::string storage_path = "testStorage";
  const size_t chunk_size = 256;
  const size_t cap_B = 2;


  using namespace dariadb::storage;

  {
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    Options::start();
    Options::instance()->strategy=dariadb::storage::STRATEGY::FAST_WRITE;
    Options::instance()->path = storage_path;
    Options::instance()->cap_B = cap_B;
    Options::instance()->cap_max_levels = 4;
    Options::instance()->aof_max_size =
        Options::instance()->measurements_count();
    dariadb::storage::Options::instance()->page_chunk_size = chunk_size;
    std::unique_ptr<Engine> stor{new Engine()};

    const size_t MEASES_SIZE = 2047 * 3 + 3;

    server_runned.store(false);
    server_stop_flag = false;
    std::thread server_thread{server_thread_func};

    while (!server_runned.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }
    server_instance->set_storage(stor.get());
    dariadb::net::client::Client c1(client_param);
    c1.connect();

    // 1 client
    while (true) {
      auto st1 = c1.state();
      dariadb::logger("ReadWriteTest test>> ", "0  state1: ", st1);
      if (st1 == dariadb::net::ClientState::WORK) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }

    dariadb::Meas::MeasArray ma;
    ma.resize(MEASES_SIZE);
    dariadb::IdArray ids;
    ids.resize(MEASES_SIZE);

    for (size_t i = 0; i < MEASES_SIZE; ++i) {
      ma[i].id = dariadb::Id(i);
      ma[i].value = dariadb::Value(i);
      ma[i].time = i;
      ids[i] = ma[i].id;
    }

    c1.write(ma);

//    while (stor->writed_count.load() != MEASES_SIZE) {
//      std::this_thread::sleep_for(std::chrono::milliseconds(300));
//    }
    dariadb::storage::QueryInterval qi{ids, 0, dariadb::Time(0),
                                       dariadb::Time(MEASES_SIZE)};
    auto result = c1.read(qi);
    BOOST_CHECK_EQUAL(result.size(), ma.size());

    dariadb::storage::QueryTimePoint qt{{ids.front()}, 0, dariadb::Time(MEASES_SIZE)};
    auto result_tp = c1.read(qt);
    BOOST_CHECK_EQUAL(result_tp.size(), size_t(1));
    BOOST_CHECK_EQUAL(result_tp[ids[0]].time, ma.front().time);

    c1.disconnect();

    while (true) {
      auto st1 = c1.state();
      dariadb::logger("ReadWriteTest test>> ", "0  state1: ", st1);
      if (st1 == dariadb::net::ClientState::DISCONNECTED) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }

    server_stop_flag = true;
    server_thread.join();
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}
