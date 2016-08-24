#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <atomic>
#include <boost/asio/steady_timer.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <iostream>
#include <net/client.h>
#include <net/server.h>
#include <thread>
#include <utils/logger.h>

const dariadb::net::Server::Param server_param(2001);
const dariadb::net::Client::Param client_param("127.0.0.1", 2001);

std::atomic_bool server_runned;
std::atomic_bool server_stop_flag;
dariadb::net::Server *server_instance = nullptr;
void server_thread_func() {
  dariadb::net::Server s(server_param);

  while (!s.is_runned()) {
  }

  server_instance = &s;
  server_runned.store(true);
  while (!server_stop_flag.load()) {
  }

  server_instance = nullptr;
}

BOOST_AUTO_TEST_CASE(Connect) {
  server_runned.store(false);
  std::thread server_thread{server_thread_func};

  while (!server_runned.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  dariadb::net::Client c(client_param);
  c.connect();

  // 1 client
  while (true) {
    auto res = server_instance->connections_accepted();
    auto st = c.state();
    dariadb::logger("test>> ", "0 count: ", res, " state: ", st);
    if (res == size_t(1) && st == dariadb::net::ClientState::WORK) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  { // this check disconnect on client::dtor.
    dariadb::net::Client c2(client_param);
    c2.connect();

    // 2 client: c and c2
    while (true) {
      auto res = server_instance->connections_accepted();
      auto st = c2.state();
      dariadb::logger("test>> ", "1 count: ", res, " state: ", st);
      if (res == size_t(2) && st == dariadb::net::ClientState::WORK) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }
  }

  // 1 client: c
  while (true) {
    auto res = server_instance->connections_accepted();
    std::cout << "one expect: " << res << std::endl;
    if (res == size_t(1)) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  dariadb::net::Client c3(client_param);
  c3.connect();

  // 2 client: c and c3
  while (true) {
    auto res = server_instance->connections_accepted();
    auto st = c3.state();
    dariadb::logger("test>> ", "2 count: ", res, " state: ", st);
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
    dariadb::logger("test>> ", "3 count: ", res, " state: ", st);
    if (res == size_t(1) &&
        c.state() == dariadb::net::ClientState::DISCONNECTED) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
  server_stop_flag = true;
  server_thread.join();
  while (true) {
    auto state = c3.state();
    dariadb::logger("test>> ", "4 state: ", state);
    if (state == dariadb::net::ClientState::DISCONNECTED) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
}
