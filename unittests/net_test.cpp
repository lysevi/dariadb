#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

#include "../network/common/net_data.h"
#include <libclient/client.h>
#include <libdariadb/engines/engine.h>
#include <libdariadb/meas.h>
#include <libdariadb/scheme/scheme.h>
#include <libdariadb/storage/wal/walfile.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>
#include <libserver/server.h>

const dariadb::net::Server::Param binary_server_param(2001, 2002);
const dariadb::net::client::Client::Param client_param("localhost", 2001, 2002);

// TODO replace by fixture
dariadb::net::Server *binary_server_instance = nullptr;

void server_thread_func() {
  dariadb::net::Server s(binary_server_param);

  EXPECT_TRUE(!s.is_runned());

  binary_server_instance = &s;
  s.start();

  binary_server_instance = nullptr;
}

TEST(Network, DataPack) {
  using dariadb::net::QueryAppend_header;
  using dariadb::net::NetData;

  NetData nd;

  auto hdr = reinterpret_cast<QueryAppend_header *>(&nd.data);
  dariadb::MeasArray ma;
  const size_t ma_sz = 5000;
  ma.resize(ma_sz);
  dariadb::Id id = 1;
  for (size_t i = 1; i < ma.size(); ++i) {
    ma[i].flag = dariadb::Flag(i);
    ma[i].value = dariadb::Value(i);
    ma[i].time = i;
    ma[i].id = id;
    if (i % 5 == 0) {
      ++id;
    }
  }

  size_t sz = 0;
  dariadb::net::QueryAppend_header::make_query(hdr, ma.data(), ma.size(), 0, &sz);
  size_t packe_count = hdr->count;

  auto readed_ma = hdr->read_measarray();
  EXPECT_EQ(packe_count, readed_ma.size());
  EXPECT_EQ(readed_ma[0].id, dariadb::Id(0));
  EXPECT_EQ(readed_ma[0].flag, dariadb::Flag(0));
  EXPECT_EQ(readed_ma[0].value, dariadb::Value(0));
  EXPECT_EQ(readed_ma[0].time, dariadb::Time(0));
  id = 1;
  for (size_t i = 1; i < readed_ma.size(); ++i) {
    EXPECT_EQ(readed_ma[i].id, dariadb::Id(id));
    EXPECT_EQ(readed_ma[i].flag, dariadb::Flag(i));
    EXPECT_EQ(readed_ma[i].value, dariadb::Value(i));
    EXPECT_EQ(readed_ma[i].time, dariadb::Time(i));
    if (i % 5 == 0) {
      ++id;
    }
  }
}

TEST(Network, Connect1) {
  dariadb::logger("********** Connect1 **********");
  std::thread server_thread{server_thread_func};

  while (binary_server_instance == nullptr || !binary_server_instance->is_runned()) {
    dariadb::utils::sleep_mls(300);
  }

  dariadb::net::client::Client c(client_param);
  c.connect();

  // 1 client
  while (true) {
    auto res = binary_server_instance->connections_accepted();
    auto st = c.state();
    dariadb::logger("Connect1 test>> ", "0 count: ", res, " state: ", st);
    if (res == size_t(1) && st == dariadb::net::CLIENT_STATE::WORK) {
      break;
    }
    dariadb::utils::sleep_mls(300);
  }

  binary_server_instance->stop();
  server_thread.join();
  while (true) {
    auto state = c.state();
    dariadb::logger("Connect1 test>> ", "4 state: ", state);
    if (state == dariadb::net::CLIENT_STATE::DISCONNECTED) {
      break;
    }
    dariadb::utils::sleep_mls(300);
  }
}

TEST(Network, Connect3) {
  dariadb::logger("********** Connect3 **********");

  std::thread server_thread{server_thread_func};

  while (binary_server_instance == nullptr || !binary_server_instance->is_runned()) {
    dariadb::utils::sleep_mls(300);
  }

  dariadb::net::client::Client c(client_param);
  c.connect();

  // 1 client
  while (true) {
    auto res = binary_server_instance->connections_accepted();
    auto st = c.state();
    dariadb::logger("Connect3 test>> ", "0 count: ", res, " state: ", st);
    if (res == size_t(1) && st == dariadb::net::CLIENT_STATE::WORK) {
      break;
    }
    dariadb::utils::sleep_mls(300);
  }

  { // this check disconnect on client::dtor.
    dariadb::net::client::Client c2(client_param);
    c2.connect();

    // 2 client: c and c2
    while (true) {
      auto res = binary_server_instance->connections_accepted();
      auto st = c2.state();
      dariadb::logger("Connect3 test>> ", "1 count: ", res, " state: ", st);
      if (res == size_t(2) && st == dariadb::net::CLIENT_STATE::WORK) {
        break;
      }
      dariadb::utils::sleep_mls(300);
    }
  }

  // 1 client: c
  while (true) {
    auto res = binary_server_instance->connections_accepted();
    dariadb::logger_fatal("Connect3 one expect: ", res);
    if (res == size_t(1)) {
      break;
    }
    dariadb::utils::sleep_mls(300);
  }

  dariadb::net::client::Client c3(client_param);
  c3.connect();

  // 2 client: c and c3
  while (true) {
    auto res = binary_server_instance->connections_accepted();
    auto st = c3.state();
    dariadb::logger("Connect3 test>> ", "2 count: ", res, " state: ", st);
    if (res == size_t(2) && st == dariadb::net::CLIENT_STATE::WORK) {
      break;
    }
    dariadb::utils::sleep_mls(300);
  }

  c.disconnect();

  // 1 client: c3
  while (true) {
    auto res = binary_server_instance->connections_accepted();
    auto st = c.state();
    dariadb::logger("Connect3 test>> ", "3 count: ", res, " state: ", st);
    if (res == size_t(1) && c.state() == dariadb::net::CLIENT_STATE::DISCONNECTED) {
      break;
    }
    dariadb::utils::sleep_mls(300);
  }
  binary_server_instance->stop();
  server_thread.join();
  while (true) {
    auto state = c3.state();
    dariadb::logger("Connect3 test>> ", "4 state: ", state);
    if (state == dariadb::net::CLIENT_STATE::DISCONNECTED) {
      break;
    }
    dariadb::utils::sleep_mls(300);
  }
}

TEST(Network, PingTest) {
  dariadb::logger("********** PingTest **********");
  const size_t MAX_PONGS = 2;
  std::thread server_thread{server_thread_func};
  while (binary_server_instance == nullptr || !binary_server_instance->is_runned()) {
    dariadb::utils::sleep_mls(300);
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
    if (st1 == dariadb::net::CLIENT_STATE::WORK &&
        st2 == dariadb::net::CLIENT_STATE::WORK) {
      break;
    }
    dariadb::utils::sleep_mls(300);
  }

  while (true) {
    auto p1 = c1.pings_answers();
    auto p2 = c2.pings_answers();
    dariadb::logger("PingTest test>> p1:", p1, " p2:", p2);
    dariadb::utils::sleep_mls(500);
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
    if (st1 == dariadb::net::CLIENT_STATE::DISCONNECTED &&
        st2 == dariadb::net::CLIENT_STATE::DISCONNECTED) {
      break;
    }
    dariadb::utils::sleep_mls(300);
  }
  binary_server_instance->stop();
  server_thread.join();
}

TEST(Network, ReadWriteTest) {
  dariadb::logger("********** ReadWriteTest **********");

  const std::string storage_path = "testStorage";
  const size_t chunk_size = 256;

  using namespace dariadb;
  using namespace dariadb::storage;

  {
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    auto settings = dariadb::storage::Settings::create(storage_path);
    settings->strategy.setValue(dariadb::STRATEGY::WAL);
    settings->chunk_size.setValue(chunk_size);
    IEngine_Ptr stor{new Engine(settings)};

    const size_t MEASES_SIZE = 2047 * 3 + 3;

    std::thread server_thread{server_thread_func};

    while (binary_server_instance == nullptr || !binary_server_instance->is_runned()) {
      dariadb::utils::sleep_mls(300);
    }
    binary_server_instance->set_storage(stor);
    dariadb::net::client::Client c1(client_param);
    c1.connect();

    // 1 client
    while (true) {
      auto st1 = c1.state();
      dariadb::logger("ReadWriteTest test>> ", "0  state1: ", st1);
      if (st1 == dariadb::net::CLIENT_STATE::WORK) {
        break;
      }
      dariadb::utils::sleep_mls(300);
    }

    dariadb::MeasArray ma;
    ma.resize(MEASES_SIZE);
    dariadb::IdArray ids;
    ids.resize(MEASES_SIZE);

    for (size_t i = 0; i < MEASES_SIZE; ++i) {
      ma[i].id = dariadb::Id(i);
      ma[i].value = dariadb::Value(i);
      ma[i].time = i;
      ids[i] = ma[i].id;
    }
    size_t subscribe_calls = 0;
    dariadb::net::client::ReadResult::callback clbk = [&subscribe_calls](
        const dariadb::net::client::ReadResult *parent, const dariadb::Meas &m,
        const dariadb::Statistic &st) { subscribe_calls++; };

    auto read_res = c1.subscribe({ma[0].id}, dariadb::Flag(0), clbk);
    read_res->wait();
    read_res = c1.subscribe({ma[1].id}, dariadb::Flag(0), clbk);
    read_res->wait();
    c1.append(ma);

    dariadb::QueryInterval qi{ids, 0, dariadb::Time(0), dariadb::Time(MEASES_SIZE)};
    auto result = c1.readInterval(qi);
    EXPECT_EQ(result.size(), ma.size());

    dariadb::QueryTimePoint qt{{ids.front()}, 0, dariadb::Time(MEASES_SIZE)};
    auto result_tp = c1.readTimePoint(qt);
    EXPECT_EQ(result_tp.size(), size_t(1));
    EXPECT_EQ(result_tp[ids[0]].time, ma.front().time);

    auto result_cv = c1.currentValue({ids[0], ids[1]}, 0);
    EXPECT_EQ(result_cv.size(), size_t(2));

    EXPECT_EQ(subscribe_calls, size_t(2));

    c1.disconnect();

    while (true) {
      auto st1 = c1.state();
      dariadb::logger("ReadWriteTest test>> ", "0  state1: ", st1);
      if (st1 == dariadb::net::CLIENT_STATE::DISCONNECTED) {
        break;
      }
      dariadb::utils::sleep_mls(300);
    }
    binary_server_instance->stop();
    server_thread.join();
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

TEST(Network, SchemeTest) {
  dariadb::logger("********** SchemeTest **********");

  using namespace dariadb;
  using namespace dariadb::storage;

  {
    auto settings = dariadb::storage::Settings::create();
    auto data_scheme = dariadb::scheme::Scheme::create(settings);
    IEngine_Ptr stor{new Engine(settings)};
    stor->setScheme(data_scheme);

    std::thread server_thread{server_thread_func};

    while (binary_server_instance == nullptr || !binary_server_instance->is_runned()) {
      dariadb::utils::sleep_mls(300);
    }
    binary_server_instance->set_storage(stor);
    dariadb::net::client::Client c1(client_param);
    c1.connect();

    // 1 client
    while (true) {
      auto st1 = c1.state();
      dariadb::logger("SchemeTest test>> ", "0  state1: ", st1);
      if (st1 == dariadb::net::CLIENT_STATE::WORK) {
        break;
      }
      dariadb::utils::sleep_mls(300);
    }

    EXPECT_TRUE(c1.addToScheme("test1"));
    EXPECT_TRUE(c1.addToScheme("test2"));
    EXPECT_TRUE(c1.addToScheme("test3"));

    auto v2id = c1.loadScheme();
    EXPECT_EQ(v2id.size(), size_t(3));

    EXPECT_TRUE(v2id.find("test1") != v2id.end());
    EXPECT_TRUE(v2id.find("test2") != v2id.end());
    EXPECT_TRUE(v2id.find("test3") != v2id.end());

    c1.disconnect();

    while (true) {
      auto st1 = c1.state();
      dariadb::logger("RepackTest test>> ", "0  state1: ", st1);
      if (st1 == dariadb::net::CLIENT_STATE::DISCONNECTED) {
        break;
      }
      dariadb::utils::sleep_mls(300);
    }
    binary_server_instance->stop();
    server_thread.join();
  }
}
