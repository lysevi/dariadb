#include <iostream>
#include <chrono>
#include <thread>

#include <libdariadb/timeutil.h>
#include <libdariadb/engine.h>
#include <libdariadb/meas.h>
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/locker.h>
#include <libclient/client.h>
#include <libserver/server.h>

#include <boost/program_options.hpp>

using namespace dariadb;
using namespace dariadb::storage;

namespace po = boost::program_options;

std::string storage_path = "dariadb_storage";

unsigned short server_port = 2001;
std::string server_host = "127.0.0.1";
bool run_server_flag = true;
size_t server_threads_count = dariadb::net::SERVER_IO_THREADS_DEFAULT;
STRATEGY strategy = STRATEGY::FAST_WRITE;
Time cap_store_period = 0;
size_t clients_count = 5;

Engine* engine = nullptr;
dariadb::net::Server*server_instance = nullptr;

void run_server() {

	bool is_exists = false;
	if (dariadb::utils::fs::path_exists(storage_path)) {
		is_exists = true;
	}

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

	engine = new Engine();

	if (!is_exists) {
		Options::instance()->save();
	}

	dariadb::net::Server::Param server_param(server_port, server_threads_count);
	server_instance=new dariadb::net::Server(server_param);
	server_instance->set_storage(engine);

	server_instance->start();

	while (!server_instance->is_runned()) {
	}
}

const size_t MEASES_SIZE = 10;
const size_t SEND_COUNT = 100;
typedef std::shared_ptr<dariadb::net::client::Client> Client_Ptr;
std::list<float> elapsed_list;
dariadb::utils::Locker e_lock;
void write_thread(Client_Ptr client) {
	dariadb::Meas::MeasArray ma;
	ma.resize(MEASES_SIZE);
	for (size_t i = 0; i < MEASES_SIZE; ++i) {
		ma[i].id = dariadb::Id(i);
		ma[i].value = dariadb::Value(i);
		ma[i].time = i;
	}


	for(size_t i=0;i<SEND_COUNT;++i){
		client->append(ma);
	}
}

int main(int argc,char**argv){
	po::options_description desc("Allowed options");
	desc.add_options()
		("help", "produce help message")
		("readonly", "readonly mode")
		("storage-path", po::value<std::string>(&storage_path)->default_value(storage_path), "path to storage.")
		("port", po::value<unsigned short>(&server_port)->default_value(server_port), "server port.")
		("server-host", po::value<std::string>(&server_host)->default_value(server_host), "server host.")
		("io_threads", po::value<size_t>(&server_threads_count)->default_value(server_threads_count), "server threads for query processing.")
		("strategy", po::value<STRATEGY>(&strategy)->default_value(strategy),"write strategy.")
		("store-period",po::value<Time>(&cap_store_period)->default_value(cap_store_period), "store period in CAP level.")
		("clients-count", po::value<size_t>(&clients_count)->default_value(clients_count), "clients count.")
		("extern-server", "dont run server.");

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
	if (vm.count("extern-server")) {
		run_server_flag = true;
	}

	
	if (run_server_flag) {
		run_server();
	}
	
	dariadb::net::client::Client::Param p(server_host, server_port);
	
	std::vector<Client_Ptr> clients(clients_count);
	for (size_t i = 0; i < clients_count; ++i) {
		Client_Ptr c{ new dariadb::net::client::Client(p) };
		c->connect();
		clients[i] = c;
	}

	auto start = clock();
	std::vector<std::thread> threads(clients_count);
	for (size_t i = 0; i < clients_count; ++i) {
		threads[i] = std::move(std::thread{ write_thread, clients[i] });
	}

	for (size_t i = 0; i < clients_count; ++i) {
		threads[i].join();
		clients[i]->disconnect();
		while (clients[i]->state() != dariadb::net::ClientState::DISCONNECTED) {
			std::this_thread::yield();
		}
	}
	auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);

	if(run_server_flag){
		server_instance->stop();

		while (server_instance->is_runned()) {
			std::this_thread::yield();
		}

		delete engine;
	}

	auto total_writed = MEASES_SIZE*SEND_COUNT*clients_count;
	std::cout << "writed: " << total_writed << std::endl;
	std::cout << "write time: " << elapsed << std::endl;
	std::cout << "speed " << total_writed /elapsed<<" per sec." << std::endl;
}