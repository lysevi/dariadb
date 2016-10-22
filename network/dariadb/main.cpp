#include <iostream>
#include <libdariadb/timeutil.h>
#include <libdariadb/engine.h>
#include <libdariadb/meas.h>
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/fs.h>
#include <libserver/server.h>

#include <boost/program_options.hpp>

#include "server_logger.h"

using namespace dariadb;
using namespace dariadb::storage;

namespace po = boost::program_options;

std::string storage_path = "dariadb_storage";
unsigned short server_port = 2001;
size_t server_threads_count = dariadb::net::SERVER_IO_THREADS_DEFAULT;
STRATEGY strategy = STRATEGY::COMPRESSED;
ServerLogger::Params p;
size_t compact_to=0;

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
        ("compact-to", po::value<size_t>(&compact_to)->default_value(compact_to), "compact all pages and exit.")
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

	
	dariadb::utils::ILogger_ptr log_ptr{ new ServerLogger(p) };
	dariadb::utils::LogManager::start(log_ptr);

	auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
	settings->strategy = strategy;


	auto stor=new Engine(settings);

    if(compact_to!=0){
        stor->compactTo(compact_to);
        delete stor;
        return 0;
    }
	dariadb::net::Server::Param server_param(server_port, server_threads_count);
	dariadb::net::Server s(server_param);
	s.set_storage(stor);

	s.start();

	while (!s.is_runned()) {
	}
	
	while (s.is_runned()){
		s.asio_run();
	}
	delete stor;
}
