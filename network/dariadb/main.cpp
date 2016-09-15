#include <iostream>
#include <libdariadb/timeutil.h>
#include <libdariadb/engine.h>
#include <libdariadb/meas.h>
#include <libdariadb/utils/logger.h>
#include <libserver/server.h>

#include <boost/program_options.hpp>

using namespace dariadb;
using namespace dariadb::storage;

class ServerLogger : public dariadb::utils::ILogger {
public:
	void message(dariadb::utils::LOG_MESSAGE_KIND kind, const std::string &msg) {
		auto ct = dariadb::timeutil::current_time();
		auto ct_str = dariadb::timeutil::to_string(ct);
		std::stringstream ss;
		ss << ct_str << " ";
		switch (kind) {
		case dariadb::utils::LOG_MESSAGE_KIND::FATAL:
			ss << "[err] " << msg << std::endl;
			break;
		case dariadb::utils::LOG_MESSAGE_KIND::INFO:
			ss << "[inf] " << msg << std::endl;
			break;
		case dariadb::utils::LOG_MESSAGE_KIND::MESSAGE:
			ss << "[dbg] " << msg << std::endl;
			break;
		}
		std::cout << ss.str();
	}
};


namespace po = boost::program_options;

std::string storage_path = "dariadb_storage";
unsigned short server_port = 2001;
//bool logg2stdout = false;
size_t server_threads_count = dariadb::net::SERVER_IO_THREADS_DEFAULT;
STRATEGY strategy = STRATEGY::DYNAMIC;
Time cap_store_period = 0;

int main(int argc,char**argv){
	po::options_description desc("Allowed options");
	desc.add_options()
		("help", "produce help message")
		("readonly", "readonly mode")
		("storage-path", po::value<std::string>(&storage_path)->default_value(storage_path), "path to storage.")
		("port", po::value<unsigned short>(&server_port)->default_value(server_port), "server port.")
		("io_threads", po::value<size_t>(&server_threads_count)->default_value(server_threads_count), "server threads for query processing.")
		("strategy", po::value<STRATEGY>(&strategy)->default_value(strategy),"write strategy.")
		("store-period",po::value<Time>(&cap_store_period)->default_value(cap_store_period), "store period in CAP level.");
		//("log-to-stdout", "logger print message to stdout.");

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
	/*if (vm.count("log-to-stdout")) {
		logg2stdout = true;
	}*/

	bool is_exists = false;
	if (dariadb::utils::fs::path_exists(storage_path)) {
		is_exists = true;
	}
	
	dariadb::utils::ILogger_ptr log_ptr{ new ServerLogger };
	dariadb::utils::LogManager::start(log_ptr);

	if (is_exists) {
		Options::start(storage_path);
	}
	else {
		Options::start();
	}
	Options::instance()->cap_store_period = cap_store_period;
	Options::instance()->strategy = strategy;
	Options::instance()->path = storage_path;
	Options::instance()->aof_max_size = Options::instance()->measurements_count();

	std::unique_ptr<Engine> stor{ new Engine() };

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
		std::this_thread::yield();
	}
}