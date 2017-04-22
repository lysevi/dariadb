#include <chrono>
#include <iostream>
#include <numeric>
#include <thread>

#include <libclient/client.h>
#include <libdariadb/engines/engine.h>
#include <libdariadb/meas.h>
#include <libdariadb/scheme/scheme.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/statistic/calculator.h>
#include <libdariadb/utils/async/locker.h>
#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>
#include <libserver/server.h>
#include <common/http_helpers.h>

#include <boost/asio.hpp>
#include <boost/program_options.hpp>
#include <extern/json/src/json.hpp>

using namespace dariadb;
using namespace dariadb::storage;

namespace po = boost::program_options;

unsigned short server_port = 2001;
unsigned short server_http_port = 8080;
std::string server_host = "localhost";
bool run_server_flag = true;
size_t server_threads_count = dariadb::net::SERVER_IO_THREADS_DEFAULT;
size_t clients_count = 5;
bool dont_clean = false;
bool http_benchmark = false;
IEngine_Ptr engine = nullptr;
dariadb::net::Server *server_instance = nullptr;

using boost::asio::ip::tcp;

void run_server() {
  auto settings = dariadb::storage::Settings::create();
  auto scheme = dariadb::scheme::Scheme::create(settings);
  engine = IEngine_Ptr{new Engine(settings)};
  engine->setScheme(scheme);

  dariadb::net::Server::Param server_param(server_port, server_http_port,
                                           server_threads_count);
  server_instance = new dariadb::net::Server(server_param);
  server_instance->set_storage(engine);

  server_instance->start();
  delete server_instance;
  server_instance = nullptr;
}

size_t MEASES_SIZE = 20000;
size_t SEND_COUNT = 10;

std::vector<double> elapsed;
std::vector<std::thread> threads;
std::vector<dariadb::net::client::Client_Ptr> clients;

void write_thread(dariadb::net::client::Client_Ptr client, size_t thread_num) {
  dariadb::MeasArray ma;
  ma.resize(MEASES_SIZE);
  dariadb::Time t = dariadb::MIN_TIME;
  dariadb::utils::ElapsedTime et;
  for (size_t i = 0; i < SEND_COUNT; ++i) {
    for (size_t j = 0; j < MEASES_SIZE; ++j) {
      ma[j].id = dariadb::Id(thread_num);
      ma[j].value = dariadb::Value(j);
      ma[j].time = t++;
    }

    client->append(ma);
  }
  auto el = et.elapsed();
  elapsed[thread_num] = el;
}

void write_http_thread(size_t thread_num) {
  auto http_port = std::to_string(server_http_port);
  using nlohmann::json;
  boost::asio::io_service test_service;
  dariadb::Time t = dariadb::MIN_TIME;

  dariadb::MeasArray ma;
  ma.resize(MEASES_SIZE);
  dariadb::utils::ElapsedTime et;
  for (size_t i = 0; i < SEND_COUNT; ++i) {

    for (size_t j = 0; j < MEASES_SIZE; ++j) {
      ma[j].id = dariadb::Id(thread_num);
      ma[j].value = dariadb::Value(j);
      ma[j].time = t++;
    }

    json js;
    js["type"] = "append";
    json js_query;

    std::vector<dariadb::Flag> flags;
    std::vector<dariadb::Value> vals;
    std::vector<dariadb::Time> times;

    for (auto v : ma) {
      vals.push_back(v.value);
      flags.push_back(v.flag);
      times.push_back(v.time);
    }

    json ids_value;
    ids_value["F"] = flags;
    ids_value["V"] = vals;
    ids_value["T"] = times;

    js_query[std::to_string(thread_num)] = ids_value;

    js["append_values"] = js_query;
    std::string query = js.dump();

    auto post_result = net::http::POST(test_service, http_port, query);
    if (post_result.code != 200) {
      THROW_EXCEPTION("http result is not ok.");
    }
  }
  auto el = et.elapsed();
  elapsed[thread_num] = el;
}

int main(int argc, char **argv) {
  po::options_description desc("Allowed options");
  auto aos = desc.add_options();
  aos("help", "produce help message");
  aos("port", po::value<unsigned short>(&server_port)->default_value(server_port),
      "server port.");
  aos("server-host", po::value<std::string>(&server_host)->default_value(server_host),
      "server host.");
  aos("io-threads",
      po::value<size_t>(&server_threads_count)->default_value(server_threads_count),
      "server threads for query processing.");
  aos("clients-count", po::value<size_t>(&clients_count)->default_value(clients_count),
      "clients count.");
  aos("dont-clean", po::value<bool>(&dont_clean)->default_value(dont_clean),
      "dont clean folder with storage if exists.");
  aos("extern-server", "dont run server.");
  aos("http-benchmark", "benchmark for http query engine.");

  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
  } catch (std::exception &ex) {
    dariadb::logger("Error: ", ex.what());
    exit(1);
  }
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    std::exit(0);
  }

  if (vm.count("extern-server")) {
    std::cout << "extern-server" << std::endl;
    run_server_flag = false;
  }

  if (vm.count("http-benchmark")) {
    std::cout << "http-benchmark" << std::endl;
    http_benchmark = true;
    /*MEASES_SIZE = 2000;
    SEND_COUNT = 100;*/
  }

  elapsed.resize(clients_count);
  threads.resize(clients_count);
  clients.resize(clients_count);
  std::thread server_thread;
  if (run_server_flag) {
    server_thread = std::move(std::thread{run_server});
  }

  while (server_instance == nullptr || !server_instance->is_runned()) {
    std::cout << "Wait server..." << std::endl;
    dariadb::utils::sleep_mls(100);
  }
  dariadb::net::client::Client::Param p(server_host, server_port, server_http_port);

  if (!http_benchmark) {
    for (size_t i = 0; i < clients_count; ++i) {
      clients[i] = dariadb::net::client::Client_Ptr{new dariadb::net::client::Client(p)};
      clients[i]->connect();
    }
  }
  for (size_t i = 0; i < clients_count; ++i) {
    if (http_benchmark) {
      auto t = std::thread{write_http_thread, i};
      threads[i] = std::move(t);
    } else {
      auto t = std::thread{write_thread, clients[i], i};
      threads[i] = std::move(t);
    }
  }

  for (size_t i = 0; i < clients_count; ++i) {
    threads[i].join();
    if (!http_benchmark) {
      clients[i]->disconnect();
      while (clients[i]->state() != dariadb::net::CLIENT_STATE::DISCONNECTED) {
        std::this_thread::yield();
      }
    }
  }

  std::cout << "write end. create binary client for reading" << std::endl;
  dariadb::net::client::Client_Ptr c{new dariadb::net::client::Client(p)};
  c->connect();
  dariadb::IdArray ids;
  if (http_benchmark) {
    auto name_map = engine->getScheme()->ls();

    for (auto kv : name_map) {
      ids.push_back(kv.first);
    }
  } else {
    ids.resize(clients_count);
    for (size_t i = 0; i < clients_count; ++i) {
      ids[i] = dariadb::Id(i);
    }
  }
  dariadb::QueryInterval ri(ids, 0, 0, MEASES_SIZE * SEND_COUNT);
  auto read_start = clock();
  auto result = c->readInterval(ri);
  auto read_end = clock();
  c->disconnect();

  double statistic_total_elapsed=0;
  if (http_benchmark) {
    auto name_map = engine->getScheme()->ls();
    boost::asio::io_service test_service;
    auto http_port=std::to_string(server_http_port);
    for(auto n:name_map){
        using nlohmann::json;
        json stat_js;
        stat_js["type"] = "statistic";
        stat_js["id"] = n.second.name;
        stat_js["from"] = dariadb::MIN_TIME;
        stat_js["to"] = dariadb::MAX_TIME;
        stat_js["flag"] = dariadb::Flag();
        stat_js["functions"] = dariadb::statistic::FunctionFactory::functions();
        auto stat_query=stat_js.dump(1);
        dariadb::utils::ElapsedTime et;
        auto post_result = net::http::POST(test_service, http_port, stat_query);
        if (post_result.code != 200) {
          THROW_EXCEPTION("http result is not ok.");
        }
        auto el = et.elapsed();
        statistic_total_elapsed+=el;
    }
    statistic_total_elapsed/=name_map.size();
  }
  if (run_server_flag) {
    server_instance->stop();

    server_thread.join();

    engine = nullptr;
  }

  auto count_per_thread = MEASES_SIZE * SEND_COUNT;
  auto total_writed = count_per_thread * clients_count;
  std::cout << "writed: " << total_writed << std::endl;

  auto summary_time = std::accumulate(elapsed.begin(), elapsed.end(), 0.0);

  auto average_time = summary_time / clients_count;
  auto summary_speed = summary_time / total_writed;

  std::cout << "summary time: " << summary_time << " sec." << std::endl;
  std::cout << "average time: " << average_time << " sec." << std::endl;
  std::cout << "average speed: " << count_per_thread / (float)(average_time)
            << " per sec." << std::endl;
  std::cout << "summary speed: " << summary_speed << " per sec." << std::endl;
  std::cout << "read speed: "
            << result.size() / (((float)read_end - read_start) / CLOCKS_PER_SEC)
            << " per sec." << std::endl;
  std::cout << "average statistic time: " << statistic_total_elapsed << " sec." << std::endl;
  std::cout << "readed:" << result.size() << std::endl;

  if (result.size() != MEASES_SIZE * clients_count * SEND_COUNT) {
    THROW_EXCEPTION("result.size()!=MEASES_SIZE*clients_count: ", result.size());
  }
}
