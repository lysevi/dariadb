#include <iostream>
#include <libdariadb/timeutil.h>
#include <libdariadb/engine.h>
#include <libdariadb/meas.h>
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/fs.h>
#include <libserver/server.h>

#include <boost/program_options.hpp>

#include "logger.h"

using namespace dariadb;
using namespace dariadb::storage;

namespace po = boost::program_options;

std::string storage_path = "dariadb_storage";
unsigned short server_port = 2001;
size_t server_threads_count = dariadb::net::SERVER_IO_THREADS_DEFAULT;
STRATEGY strategy = STRATEGY::COMPRESSED;
ServerLogger::Params p;

int main(int argc,char**argv){
	po::options_description desc("Allowed options");
	desc.add_options()
		("help", "produce help message")
		("readonly", "readonly mode")
		("log-to-file", "logger out to dariadb.log.")
		("color-log", "use colors to log to console.")
		("dbg-log", "verbose logging.")
		("storage-path", po::value<std::string>(&storage_path)->default_value(storage_path), "path to storage.")
		("port", po::value<unsigned short>(&server_port)->default_value(server_port), "server port.")
		("io-threads", po::value<size_t>(&server_threads_count)->default_value(server_threads_count), "server threads for query processing.")
		("strategy", po::value<STRATEGY>(&strategy)->default_value(strategy),"write strategy.");		
	
	po::variables_map vm;
	try {
		po::store(po::parse_command_line(argc, argv, desc), vm);
	}
	catch (std::exception &ex) {
		dariadb::logger("Error: ", ex.what());
		exit(1);
	}
	po::notify(vm);

	if (vm.count("help")) {
		std::cout << desc << std::endl;
		std::exit(0);
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

	bool is_exists = false;
	if (dariadb::utils::fs::path_exists(storage_path)) {
		is_exists = true;
	}
	
	dariadb::utils::ILogger_ptr log_ptr{ new ServerLogger(p) };
	dariadb::utils::LogManager::start(log_ptr);

	if (is_exists) {
		Options::start(storage_path);
	}
	else {
		Options::start();
	}
	Options::instance()->strategy = strategy;
	Options::instance()->path = storage_path;

	auto stor=std::make_unique<Engine>();

	if (!is_exists) {
		Options::instance()->save();
	}

	dariadb::net::Server::Param server_param(server_port, server_threads_count);
	dariadb::net::Server s(server_param);
	s.set_storage(stor.get());

	s.start();

	while (!s.is_runned()) {
	}
	
	while (s.is_runned()){
		s.asio_run();
	}
}
