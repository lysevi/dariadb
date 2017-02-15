#include <chrono>
#include <iostream>
#include <numeric>
#include <thread>

#include <libclient/client.h>
#include <libdariadb/engine.h>
#include <libdariadb/meas.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/async/locker.h>
#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>
#include <libserver/server.h>

#include <boost/program_options.hpp>

using namespace dariadb;
using namespace dariadb::storage;

namespace po = boost::program_options;

std::string storage_path = "dariadb_storage";

unsigned short server_port = 2001;
std::string server_host = "localhost";
bool run_server_flag = true;
size_t server_threads_count = dariadb::net::SERVER_IO_THREADS_DEFAULT;
STRATEGY strategy = STRATEGY::WAL;
size_t clients_count = 5;
bool dont_clean = false;
Engine *engine = nullptr;
dariadb::net::Server *server_instance = nullptr;

void run_server() {

  bool is_exists = false;
  if (dariadb::utils::fs::path_exists(storage_path)) {

    if (!dont_clean) {
      std::cout << "remove old storage..." << std::endl;
      dariadb::utils::fs::rm(storage_path);
    } else {
      is_exists = true;
    }
  }

  auto settings = dariadb::storage::Settings::create(storage_path);
  settings->strategy.setValue(strategy);

  engine = new Engine(settings);

  if (!is_exists) {
    settings->save();
  }

  dariadb::net::Server::Param server_param(server_port, server_threads_count);
  server_instance = new dariadb::net::Server(server_param);
  server_instance->set_storage(engine);

  server_instance->start();

  while (!server_instance->is_runned()) {
  }
}

const size_t MEASES_SIZE = 200000;
const size_t SEND_COUNT = 1;

std::vector<float> elapsed;
std::vector<std::thread> threads;
std::vector<dariadb::net::client::Client_Ptr> clients;

void write_thread(dariadb::net::client::Client_Ptr client, size_t thread_num) {
  dariadb::MeasArray ma;
  ma.resize(MEASES_SIZE);
  for (size_t i = 0; i < MEASES_SIZE; ++i) {
    ma[i].id = dariadb::Id(thread_num);
    ma[i].value = dariadb::Value(i);
    ma[i].time = i;
  }

  auto start = clock();
  for (size_t i = 0; i < SEND_COUNT; ++i) {
    client->append(ma);
  }
  auto el = (((float)clock() - start) / CLOCKS_PER_SEC);
  elapsed[thread_num] = el;
}

int main(int argc, char **argv) {
  po::options_description desc("Allowed options");
  auto aos = desc.add_options();
  aos("help", "produce help message");
  aos("storage-path", po::value<std::string>(&storage_path)->default_value(storage_path),
      "path to storage.");
  aos("port", po::value<unsigned short>(&server_port)->default_value(server_port),
      "server port.");
  aos("server-host", po::value<std::string>(&server_host)->default_value(server_host),
      "server host.");
  aos("io-threads",
      po::value<size_t>(&server_threads_count)->default_value(server_threads_count),
      "server threads for query processing.");
  aos("strategy", po::value<STRATEGY>(&strategy)->default_value(strategy),
      "write strategy.");
  aos("clients-count", po::value<size_t>(&clients_count)->default_value(clients_count),
      "clients count.");
  aos("dont-clean", po::value<bool>(&dont_clean)->default_value(dont_clean),
      "dont clean folder with storage if exists.");
  aos("extern-server", "dont run server.");

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
    run_server_flag = false;
  }

  elapsed.resize(clients_count);
  threads.resize(clients_count);
  clients.resize(clients_count);

  if (run_server_flag) {
    run_server();
  }

  dariadb::net::client::Client::Param p(server_host, server_port);

  for (size_t i = 0; i < clients_count; ++i) {
    clients[i] = dariadb::net::client::Client_Ptr{new dariadb::net::client::Client(p)};
    clients[i]->connect();
  }

  for (size_t i = 0; i < clients_count; ++i) {
    auto t = std::thread{write_thread, clients[i], i};
    threads[i] = std::move(t);
  }

  for (size_t i = 0; i < clients_count; ++i) {
    threads[i].join();
    clients[i]->disconnect();
    while (clients[i]->state() != dariadb::net::CLIENT_STATE::DISCONNECTED) {
      std::this_thread::yield();
    }
  }
  dariadb::net::client::Client_Ptr c{new dariadb::net::client::Client(p)};
  c->connect();
  dariadb::QueryInterval ri(dariadb::IdArray{0}, 0, 0, MEASES_SIZE);
  auto read_start = clock();
  auto result = c->readInterval(ri);
  auto read_end = clock();
  c->disconnect();

  if (run_server_flag) {
    server_instance->stop();

    while (server_instance->is_runned()) {
      std::this_thread::yield();
    }

    delete engine;
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

  if (result.size() != MEASES_SIZE * SEND_COUNT) {
    THROW_EXCEPTION("result.size()!=MEASES_SIZE*SEND_COUNT: ", result.size());
  }
}
