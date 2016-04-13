#include <ctime>
#include <iostream>
#include <cstdlib>
#include <iterator>

#include <dariadb.h>
#include <utils/fs.h>
#include <storage/memstorage.h>
#include <storage/capacitor.h>
#include <ctime>
#include <limits>
#include <cmath>
#include <chrono>
#include <thread>
#include <atomic>
#include "bench_common.h"

class BenchCallback :public dariadb::storage::ReaderClb {
public:
	void call(const dariadb::Meas&) {
		count++;
	}
	size_t count;
};

std::atomic_long append_count{ 0 }, read_all_times{ 0 };
bool stop_info = false;


bool stop_read_all{ false };

void thread_read_all(
	dariadb::Time from,
	dariadb::Time to,
    dariadb::storage::BaseStorage_ptr ms)
{
	auto clb = std::make_shared<BenchCallback>();
	while (!stop_read_all) {
		auto rdr = ms->readInterval(from, to);
		rdr->readAll(clb.get());
		read_all_times++;
	}
}

void show_info() {
	clock_t t0 = clock();
	auto all_writes = dariadb_bench::total_threads_count*dariadb_bench::iteration_count;
	while (true) {
		std::this_thread::sleep_for(std::chrono::milliseconds(300));

		clock_t t1 = clock();
		auto writes_per_sec = append_count.load() / double((t1 - t0) / CLOCKS_PER_SEC);
		//auto read_per_sec = read_all_times.load() / double((t1 - t0) / CLOCKS_PER_SEC);
		std::cout << "\rwrites: " << writes_per_sec
			<< "/sec progress:" << (100 * append_count) / all_writes
			<< "%     ";
		/*if (!stop_read_all) {
			std::cout << " read_all_times: " << read_per_sec <<"/sec             ";
		}*/
		std::cout.flush();
		if (stop_info) {
			/*std::cout << "\rwrites: " << writes_per_sec
				<< "/sec progress:" << (100 * append_count) / all_writes
				<< "%            ";
			if (!stop_read_all) {
				std::cout << " read_all_times: " << read_per_sec << "/sec            ";
			}*/
			std::cout.flush();
			break;
		}
	}
	std::cout << "\n";
}

int main(int argc, char *argv[]) {
	(void)argc;
	(void)argv;
	{
		std::cout << "MemStorage" << std::endl;
        dariadb::storage::BaseStorage_ptr ms{ new dariadb::storage::MemoryStorage{ 2000000 } };
		std::thread info_thread(show_info);
		std::vector<std::thread> writers(dariadb_bench::total_threads_count);
		size_t pos = 0;
		for (size_t i = 0; i < dariadb_bench::total_threads_count; i++) {
			std::thread t{ dariadb_bench::thread_writer_rnd_stor, i,dariadb::Time(i+1),&append_count, ms };
			writers[pos++] = std::move(t);
		}
		//std::thread read_all_t{ thread_read_all, 0, dariadb::Time(iteration_count), ms };

		pos = 0;
		for (size_t i = 0; i < dariadb_bench::total_threads_count; i++) {
			std::thread t = std::move(writers[pos++]);
			t.join();
		}
		stop_info = true;
		stop_read_all = true;
		info_thread.join();
		//read_all_t.join();
	}
	{
		std::cout << "Capacitor" << std::endl;
        dariadb::storage::BaseStorage_ptr ms{ new dariadb::storage::MemoryStorage{ 2000000 } };
		std::unique_ptr<dariadb::storage::Capacitor> cp{ 
			new dariadb::storage::Capacitor(ms, dariadb::storage::Capacitor::Params(1000,1000))
		};

		append_count = 0;
		stop_info = false;

		std::thread info_thread(show_info);
		std::vector<std::thread> writers(dariadb_bench::total_threads_count);
		size_t pos = 0;
		for (size_t i = 0; i < dariadb_bench::total_threads_count; i++) {
			std::thread t{ dariadb_bench::thread_writer_rnd_stor, i,dariadb::Time(i),&append_count, ms };
			writers[pos++] = std::move(t);
		}

		pos = 0;
		for (size_t i = 0; i < dariadb_bench::total_threads_count; i++) {
			std::thread t = std::move(writers[pos++]);
			t.join();
		}
		stop_info = true;
		info_thread.join();
		cp->flush();
	}
	{
		std::cout << "Union" << std::endl;
		const std::string storage_path = "testStorage";
		const size_t chunk_per_storage = 1024;
		const size_t chunk_size = 512;
		const size_t cap_max_size = 10000;
		const dariadb::Time write_window_deep = 2000;
		const dariadb::Time old_mem_chunks = 0;
		const size_t max_mem_chunks = 0;

		if (dariadb::utils::fs::path_exists(storage_path)) {
			dariadb::utils::fs::rm(storage_path);
		}

        dariadb::storage::BaseStorage_ptr ms{
			new dariadb::storage::UnionStorage(
			dariadb::storage::PageManager::Params(storage_path, dariadb::storage::MODE::SINGLE, chunk_per_storage, chunk_size),
				dariadb::storage::Capacitor::Params(cap_max_size,write_window_deep),
				dariadb::storage::UnionStorage::Limits(max_mem_chunks, old_mem_chunks)) };

		append_count = 0;
		stop_info = false;
		stop_read_all = false;

		std::thread info_thread(show_info);
		std::vector<std::thread> writers(dariadb_bench::total_threads_count);

		size_t pos = 0;
		for (size_t i = 0; i < dariadb_bench::total_threads_count; i++) {
			std::thread t{ dariadb_bench::thread_writer_rnd_stor, i,dariadb::Time(i),&append_count, ms };
			writers[pos++] = std::move(t);
		}
		//std::thread read_all_t{ thread_read_all, 0, dariadb::Time(iteration_count), ms };

		pos = 0;
		for (size_t i = 0; i < dariadb_bench::total_threads_count; i++) {
			std::thread t = std::move(writers[pos++]);
			t.join();
		}
		stop_info = true;
		stop_read_all = true;
		info_thread.join();
		//read_all_t.join();
		ms = nullptr;
		if (dariadb::utils::fs::path_exists(storage_path)) {
			dariadb::utils::fs::rm(storage_path);
		}
	}
}
