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

std::atomic_long append_count{ 0 };
size_t total_threads_count = 4;
size_t iteration_count = 1000000;
bool stop_info = false;


void thread_writer_rnd_stor(dariadb::Id id, dariadb::Time sleep_time,
    dariadb::storage::BaseStorage_ptr ms)
{
	auto sleep_duration = std::chrono::milliseconds(sleep_time);
	auto m = dariadb::Meas::empty();
	m.time = dariadb::timeutil::current_time() - id;
	for (size_t i = 0; i < iteration_count; i++) {
		m.id = id;
		m.flag = dariadb::Flag(id);
		m.time+= sleep_time;
		m.value = dariadb::Value(i);
		ms->append(m);
		append_count++;
		//std::this_thread::sleep_for(sleep_duration);
	}
}


void show_info() {
	clock_t t0 = clock();
	auto all_writes = total_threads_count*iteration_count;
	while (true) {
		std::this_thread::sleep_for(std::chrono::milliseconds(300));

		clock_t t1 = clock();
		auto writes_per_sec = append_count.load() / double((t1 - t0) / CLOCKS_PER_SEC);
		std::cout << "\rwrites: " <<append_count
			<< " speed:"	<< writes_per_sec << "/sec progress:" 
			<< (100 * append_count) / all_writes << "%     ";
		std::cout.flush();
		if (stop_info) {
			std::cout.flush();
			break;
		}
	}
	std::cout << "\n";
}

int main(int argc, char *argv[]) {
	(void)argc;
	(void)argv;
	std::cout << "Performance benchmark" << std::endl;
	{
		std::cout << "write..." << std::endl;
		const std::string storage_path = "testStorage";
		const size_t chunk_per_storage = 1024*1024;
		const size_t chunk_size = 256;
		const size_t cap_max_size = 50;
		const dariadb::Time write_window_deep = 500;
		const dariadb::Time old_mem_chunks = 0;
		const size_t max_mem_chunks = 100;

		if (dariadb::utils::fs::path_exists(storage_path)) {
			dariadb::utils::fs::rm(storage_path);
		}

        dariadb::storage::BaseStorage_ptr ms{
			new dariadb::storage::UnionStorage(storage_path,
                                               dariadb::storage::MODE::SINGLE,
											   chunk_per_storage,
											   chunk_size,
											   write_window_deep,
											   cap_max_size,old_mem_chunks,max_mem_chunks) };

		append_count = 0;
		stop_info = false;

		std::thread info_thread(show_info);
		
		std::vector<std::thread> writers(total_threads_count);

		size_t pos = 0;
		for (size_t i = 1; i < total_threads_count+1; i++) {
			std::thread t{ thread_writer_rnd_stor, pos, dariadb::Time(i), ms };
			writers[pos++] = std::move(t);
		}
		
		pos = 0;
		for (size_t i = 0; i < total_threads_count; i++) {
			std::thread t = std::move(writers[pos++]);
			t.join();
		}
		stop_info = true;
		info_thread.join();
		ms = nullptr;
		if (dariadb::utils::fs::path_exists(storage_path)) {
			dariadb::utils::fs::rm(storage_path);
		}
	}
}
