#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <atomic>
#include <boost/asio/steady_timer.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <interfaces/imeasstorage.h>
#include <iostream>
#include <meas.h>
#include <net/client.h>
#include <net/server.h>
#include <thread>
#include <utils/logger.h>

class Mock_MeasStorage : public virtual dariadb::storage::IMeasStorage {
public:
  Mock_MeasStorage() {
	  writed_count.store(0);
  }

  virtual dariadb::Time minTime() override { return dariadb::Time(0); }
  virtual dariadb::Time maxTime() override { return dariadb::Time(0); }

  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) {
    return false;
  }

  virtual void foreach (const dariadb::storage::QueryInterval &,
                        dariadb::storage::IReaderClb *) override {}
  virtual dariadb::Meas::Id2Meas
  readInTimePoint(const dariadb::storage::QueryTimePoint &q) override {
    return dariadb::Meas::Id2Meas{};
  }
  virtual dariadb::Meas::Id2Meas currentValue(const dariadb::IdArray &ids,
                                              const dariadb::Flag &flag) override {
    return dariadb::Meas::Id2Meas{};
  }

  virtual dariadb::append_result append(const dariadb::Meas &value) override {
	  writed_count++;
	  return dariadb::append_result(1, 0);
  }
  virtual void flush() override {
  }

  std::atomic_int writed_count;
};

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
  dariadb::logger("********** Connect **********");
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
    if (res == size_t(1) && c.state() == dariadb::net::ClientState::DISCONNECTED) {
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

BOOST_AUTO_TEST_CASE(PingTest) {
  dariadb::logger("********** PingTest **********");
  const size_t MAX_PONGS = 2;
  server_runned.store(false);
  server_stop_flag = false;
  std::thread server_thread{server_thread_func};

  while (!server_runned.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  dariadb::net::Client c1(client_param);
  c1.connect();
  dariadb::net::Client c2(client_param);
  c2.connect();

  // 2 clients
  while (true) {
    auto st1 = c1.state();
    auto st2 = c2.state();
    dariadb::logger("test>> ", "0  state1: ", st1, " state2: ", st2);
    if (st1 == dariadb::net::ClientState::WORK &&
        st2 == dariadb::net::ClientState::WORK) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  while (true) {
    auto p1 = c1.pings_answers();
    auto p2 = c2.pings_answers();
    dariadb::logger("test>> p1:", p1, " p2:", p2);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    if (p1 > MAX_PONGS && p2 > MAX_PONGS) {
      break;
    }
  }
  c1.disconnect();
  c2.disconnect();
  server_stop_flag = true;
  server_thread.join();
}

BOOST_AUTO_TEST_CASE(WriteTest) {
  const size_t MEASES_SIZE = 101;
  dariadb::logger("********** WriteTest **********");
  std::shared_ptr<Mock_MeasStorage> stor{ new Mock_MeasStorage() };
  server_runned.store(false);
  server_stop_flag = false;
  std::thread server_thread{server_thread_func};

  while (!server_runned.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
  server_instance->set_storage(stor.get());
  dariadb::net::Client c1(client_param);
  c1.connect();

  // 1 client
  while (true) {
    auto st1 = c1.state();
    dariadb::logger("test>> ", "0  state1: ", st1);
    if (st1 == dariadb::net::ClientState::WORK) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  dariadb::Meas::MeasArray ma;
  ma.resize(MEASES_SIZE);
  for (size_t i = 0; i < MEASES_SIZE; ++i) {
    ma[i].id = dariadb::Id(i);
    ma[i].value = dariadb::Value(i);
  }

  c1.write(ma);
  c1.disconnect();

  while (stor->writed_count.load() != MEASES_SIZE) {
	  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
  server_stop_flag = true;
  server_thread.join();
}
