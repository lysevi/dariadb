#include <chrono>
#include <iostream>
#include <numeric>
#include <thread>

#include <libclient/client.h>
#include <libdariadb/engines/engine.h>
#include <libdariadb/meas.h>
#include <libdariadb/scheme/scheme.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/async/locker.h>
#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>
#include <libserver/server.h>

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

const dariadb::net::Server::Param server_param(2001, 8080);

struct post_response {
  int code;
  std::string answer;
};

post_response post(boost::asio::io_service &service, std::string &port,
                   const std::string &json_query) {
  post_response result;
  result.code = 0;

  tcp::resolver resolver(service);
  tcp::resolver::query query("localhost", port);
  tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

  tcp::socket socket(service);
  boost::asio::connect(socket, endpoint_iterator);

  boost::asio::streambuf request;
  std::ostream request_stream(&request);

  request_stream << "POST / HTTP/1.1\r\n";
  request_stream << "Host:"
                 << " localhost:8080"
                 << "\r\n";
  request_stream << "User-Agent: C/1.0"
                 << "\r\n";
  request_stream << "Content-Type: application/json; charset=utf-8 \r\n";
  request_stream << "Accept: */*\r\n";
  request_stream << "Content-Length: " << json_query.length() << "\r\n";
  request_stream << "Connection: close\r\n\r\n"; // NOTE THE Double line feed
  request_stream << json_query;

  boost::asio::write(socket, request);

  // read answer
  boost::asio::streambuf response;
  boost::asio::read_until(socket, response, "\r\n");

  // Check that response is OK.
  std::istream response_stream(&response);
  std::string http_version;
  response_stream >> http_version;
  unsigned int status_code;
  response_stream >> status_code;
  std::string status_message;
  std::getline(response_stream, status_message);
  if (!response_stream || http_version.substr(0, 5) != "HTTP/") {
    std::cout << "Invalid response\n";
    result.code = -1;
    return result;
  }
  result.code = status_code;
  if (status_code != 200) {
    std::cout << "Response returned with status code " << status_code << "\n";
    return result;
  }

  boost::asio::read_until(socket, response, "\r\n\r\n");

  std::stringstream ss;
  // Process the response headers.
  std::string header;
  while (std::getline(response_stream, header) && header != "\r") {
    ss << header << "\n";
  }
  ss << "\n";

  if (response.size() > 0) {
    ss << &response;
  }

  boost::system::error_code error;
  while (boost::asio::read(socket, response, boost::asio::transfer_at_least(1), error)) {
    ss << &response;
  }
  result.answer = ss.str();
  return result;
}

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

size_t MEASES_SIZE = 200000;
size_t SEND_COUNT = 1;

std::vector<float> elapsed;
std::vector<std::thread> threads;
std::vector<dariadb::net::client::Client_Ptr> clients;

void write_thread(dariadb::net::client::Client_Ptr client, size_t thread_num) {
  dariadb::MeasArray ma;
  ma.resize(MEASES_SIZE);
  dariadb::Time t = dariadb::MIN_TIME;
  dariadb::utils::ElapsedTime et;
  for (size_t i = 0; i < SEND_COUNT; ++i) {
    for (size_t i = 0; i < MEASES_SIZE; ++i) {
      ma[i].id = dariadb::Id(thread_num);
      ma[i].value = dariadb::Value(i);
      ma[i].time = t++;
    }

    client->append(ma);
  }
  auto el = et.elapsed();
  elapsed[thread_num] = el;
}

void write_http_thread(size_t thread_num) {
  auto http_port = std::to_string(server_param.http_port);
  using nlohmann::json;
  boost::asio::io_service test_service;
  dariadb::Time t = dariadb::MIN_TIME;

  dariadb::MeasArray ma;
  ma.resize(MEASES_SIZE);
  dariadb::utils::ElapsedTime et;
  for (size_t i = 0; i < SEND_COUNT; ++i) {

    for (size_t i = 0; i < MEASES_SIZE; ++i) {
      ma[i].id = dariadb::Id(thread_num);
      ma[i].value = dariadb::Value(i);
      ma[i].time = t++;
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

    auto post_result = post(test_service, http_port, query);
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
	MEASES_SIZE = 2000;
	SEND_COUNT = 100;
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
  dariadb::net::client::Client::Param p(server_host, server_port);

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

  if (run_server_flag) {
    server_instance->stop();

    server_thread.join();

    engine = nullptr;
  }

  auto count_per_thread = MEASES_SIZE * SEND_COUNT;
  auto total_writed = count_per_thread * clients_count;
  std::cout << "writed: " << total_writed << std::endl;

  auto average_time =
      std::accumulate(elapsed.begin(), elapsed.end(), 0.0) / clients_count;
  std::cout << "average time: " << average_time << " sec." << std::endl;
  std::cout << "average speed: " << count_per_thread / (float)(average_time)
            << " per sec." << std::endl;
  std::cout << "read speed: "
            << result.size() / (((float)read_end - read_start) / CLOCKS_PER_SEC)
            << " per sec." << std::endl;

  std::cout << "readed:" << result.size() << std::endl;

  if (result.size() != MEASES_SIZE * clients_count * SEND_COUNT) {
    THROW_EXCEPTION("result.size()!=MEASES_SIZE*clients_count: ", result.size());
  }
}
