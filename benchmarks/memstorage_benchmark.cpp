#define _SCL_SECURE_NO_WARNINGS

#include <chrono>
#include <cmath>
#include <iostream>
#include <map>
#include <numeric>
#include <thread>
#include <vector>

#include "bench_common.h"

#include <libdariadb/ads/fixed_tree.h>
#include <libdariadb/storage/memstorage.h>
#include <libdariadb/meas.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/fs.h>

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/program_options.hpp>

namespace po = boost::program_options;

using namespace dariadb;
using namespace dariadb::storage;

std::atomic_llong append_count{ 0 };
dariadb::Time write_time = 0;
bool stop_info = false;
dariadb::storage::MemStorage* memstorage;
STRATEGY strategy = STRATEGY::MEMORY;
Time store_period = boost::posix_time::hours(dariadb_bench::hours_write_perid / 2).total_microseconds();

void show_info() {
	clock_t t0 = clock();

	while (true) {
		std::this_thread::sleep_for(std::chrono::milliseconds(100));

		clock_t t1 = clock();
		auto writes_per_sec = append_count.load() / double((t1 - t0) / CLOCKS_PER_SEC);
		auto dscr = memstorage->description();
		std::cout << "\r"
			<< " writes: " << append_count << " speed: " << writes_per_sec
			<< "/sec"
            <<" [m:"<< dscr.allocator_capacity<<", a:"<< dscr.allocated<<"]"
			<<" progress:"
			<< (int64_t(100) * append_count) / dariadb_bench::all_writes
			<< "%                ";
		std::cout.flush();
		if (stop_info) {
			std::cout.flush();
			break;
		}
	}
	std::cout << "\n";
}


int main(int argc, char **argv) {
	po::options_description desc("Allowed options");
	bool metrics_enable = false;
	auto aos = desc.add_options()("help", "produce help message");
	aos("strategy", po::value<STRATEGY>(&strategy)->default_value(strategy), "Write strategy");
	aos("enable-metrics", po::value<bool>(&metrics_enable)->default_value(metrics_enable));

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
		return 1;
	}

	if (metrics_enable) {
		std::cout << "enable metrics." << std::endl;
	}

	dariadb::IdSet all_id_set;
	{
		auto storage_path = "testMemoryStorage";
		if (dariadb::utils::fs::path_exists(storage_path)) {
			std::cout << " remove " << storage_path << std::endl;
			dariadb::utils::fs::rm(storage_path);
		}
		auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
		settings->strategy = strategy;
		settings->memory_limit = 500*1024*1024;
		settings->page_chunk_size = 1024;
		memstorage = new dariadb::storage::MemStorage{ settings };

		std::thread info_thread(show_info);

		std::vector<std::thread> writers(dariadb_bench::total_threads_count);

		size_t pos = 0;
		for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
			all_id_set.insert(pos);
			std::thread t{
				dariadb_bench::thread_writer_rnd_stor,
				dariadb::Id(pos), 
				&append_count, 
				memstorage,
				dariadb::timeutil::current_time(),     
				&write_time };
			writers[pos++] = std::move(t);
		}

		pos = 0;
		for (size_t i = 1; i < dariadb_bench::total_threads_count + 1; i++) {
			std::thread t = std::move(writers[pos++]);
			t.join();
		}

		stop_info = true;
		info_thread.join();

        dariadb_bench::readBenchark(all_id_set, memstorage, 10, false,false);

		delete memstorage;
		dariadb::utils::async::ThreadManager::stop();

		if (metrics_enable) {
			std::cout << "metrics:\n"
				<< dariadb::utils::metrics::MetricsManager::instance()->to_string()
				<< std::endl;
		}
	}
}
