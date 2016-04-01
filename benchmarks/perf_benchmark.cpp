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
size_t total_threads_count = 5;
size_t iteration_count = 1500000;
bool stop_info = false;


void thread_writer_rnd_stor(dariadb::Id id, dariadb::Time sleep_time,
    dariadb::storage::BaseStorage_ptr ms)
{
    //auto sleep_duration = std::chrono::milliseconds(sleep_time);
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


void show_info(dariadb::storage::UnionStorage *storage) {
	clock_t t0 = clock();
	auto all_writes = total_threads_count*iteration_count;
	while (true) {
		std::this_thread::sleep_for(std::chrono::milliseconds(300));

		clock_t t1 = clock();
		auto writes_per_sec = append_count.load() / double((t1 - t0) / CLOCKS_PER_SEC);
		std::cout 
			<<"\rin memory chunks: "<<storage->chunks_in_memory()
			<< " in disk chunks: " << dariadb::storage::PageManager::instance()->chunks_in_cur_page()
			<< " writes: "<<append_count
			<< " speed: "<< writes_per_sec << "/sec progress:" 
			<< (100 * append_count) / all_writes << "%                ";
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
	const std::string storage_path = "testStorage";
	{
		std::cout << "write..." << std::endl;
		
		const size_t chunk_per_storage = 1024*1024;
		const size_t chunk_size = 256;
		const size_t cap_max_size = 100;
		const dariadb::Time write_window_deep = 500;
		const dariadb::Time old_mem_chunks = 0;
		const size_t max_mem_chunks = 100;

		if (dariadb::utils::fs::path_exists(storage_path)) {
			dariadb::utils::fs::rm(storage_path);
		}

		auto raw_ptr = new dariadb::storage::UnionStorage(
			dariadb::storage::PageManager::Params(storage_path, dariadb::storage::MODE::SINGLE, chunk_per_storage, chunk_size),
			dariadb::storage::Capacitor::Params(cap_max_size, write_window_deep),
			dariadb::storage::UnionStorage::Limits(old_mem_chunks, max_mem_chunks));
		
		dariadb::storage::BaseStorage_ptr ms{raw_ptr};

		append_count = 0;
		stop_info = false;

		std::thread info_thread(show_info, raw_ptr);
		
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
		std::cout << "stoping storage...\n";
		ms = nullptr;

	}
	std::cout << "cleaning...\n";
	if (dariadb::utils::fs::path_exists(storage_path)) {
		dariadb::utils::fs::rm(storage_path);
	}
}
