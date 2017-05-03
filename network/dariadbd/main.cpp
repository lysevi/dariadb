#include <libdariadb/dariadb.h>
#include <libdariadb/utils/logger.h>
#include <libserver/server.h>
#include <iostream>
#include <sstream>

#include <boost/program_options.hpp>

#include "server_logger.h"

using namespace dariadb;
using namespace dariadb::storage;

namespace po = boost::program_options;

std::string storage_path = "dariadb_storage";
unsigned short server_port = 2001;
unsigned short server_http_port = 2002;
size_t server_threads_count = dariadb::net::SERVER_IO_THREADS_DEFAULT;
STRATEGY strategy = STRATEGY::COMPRESSED;
ServerLogger::Params p;
size_t memory_limit = 0;
bool force_unlock_storage = false;
bool memonly = false;

int main(int argc, char **argv) {
  po::options_description desc("Allowed options");
  auto aos = desc.add_options();
  aos("help", "produce help message");

  po::options_description logger_params("Logger params");
  auto log_options = logger_params.add_options();
  log_options("log-to-file", "logger out to dariadb.log.");
  log_options("color-log", "use colors to log to console.");
  log_options("dbg-log", "verbose logging.");
  desc.add(logger_params);

  po::options_description storage_params("Storage params");
  auto stor_options = storage_params.add_options();
  stor_options("memory-only", "run as memory only database.");
  stor_options("storage-path",
               po::value<std::string>(&storage_path)->default_value(storage_path),
               "path to storage.");

  stor_options("strategy", po::value<STRATEGY>(&strategy)->default_value(strategy),
               "write strategy.");
  stor_options("memory-limit",
               po::value<size_t>(&memory_limit)->default_value(memory_limit),
               "allocation area limit  in megabytes when strategy=MEMORY");
  stor_options("force-unlock", "force unlock storage.");
  desc.add(storage_params);

  po::options_description server_params("Server params");
  auto srv_options = server_params.add_options();
  srv_options("port", po::value<unsigned short>(&server_port)->default_value(server_port),
              "server port.");
  srv_options(
      "http-port",
      po::value<unsigned short>(&server_http_port)->default_value(server_http_port),
      "server http port.");
  srv_options(
      "io-threads",
      po::value<size_t>(&server_threads_count)->default_value(server_threads_count),
      "server threads for query processing.");
  desc.add(server_params);

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

  if (vm.count("memory-only")) {
    std::cout << "memory-only" << std::endl;
    memonly = true;
  }

  if (vm.count("log-to-file")) {
    p.use_stdout = false;
  }

  if (vm.count("color-log")) {
    p.color_console = true;
  }

  if (vm.count("dbg-log")) {
    p.dbg_logging = true;
  }

  if (vm.count("force-unlock")) {
    dariadb::logger_info("Force unlock storage.");
    force_unlock_storage = true;
  }

  dariadb::utils::ILogger_ptr log_ptr{new ServerLogger(p)};
  dariadb::utils::LogManager::start(log_ptr);

  std::stringstream ss;
  ss << "cmdline: ";
  for (int i = 0; i < argc; ++i) {
    ss << argv[i] << " ";
  }

  log_ptr->message(dariadb::utils::LOG_MESSAGE_KIND::INFO, ss.str());
  dariadb::storage::Settings_ptr settings;
  if (!memonly) {
    settings = dariadb::storage::Settings::create(storage_path);
    settings->strategy.setValue(strategy);
	settings->save();
  } else {
    settings = dariadb::storage::Settings::create();
  }

  if (strategy == STRATEGY::MEMORY && memory_limit != 0 && !memonly) {
    logger_info("memory limit: ", memory_limit);
    settings->memory_limit.setValue(memory_limit * 1024 * 1024);
  }
  settings->save();

  auto scheme = dariadb::scheme::Scheme::create(settings);

  IEngine_Ptr stor{new Engine(settings, true, force_unlock_storage)};
  stor->setScheme(scheme);

  auto aggregator=std::make_shared<aggregator::Aggregator>(stor);

  dariadb::net::Server::Param server_param(server_port, server_http_port,
                                           server_threads_count);
  dariadb::net::Server s(server_param);
  s.set_storage(stor);

  s.start();
  aggregator = nullptr;
}
