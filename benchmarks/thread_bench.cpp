#include <ctime>
#include <iostream>
#include <cstdlib>
#include <iterator>

#include <dariadb.h>
#include <utils/fs.h>
#include <ctime>
#include <limits>
#include <cmath>
#include <chrono>
#include <thread>
#include <atomic>

class BenchCallback :public dariadb::storage::ReaderClb {
public:
	void call(const dariadb::Meas&) {
		count++;
	}
	size_t count;
};

std::atomic_long append_count{ 0 }, read_all_times{ 0 };
size_t thread_count = 2;
size_t iteration_count = 1000000;
bool stop_info = false;

void thread_writer(dariadb::Id id,
	dariadb::Time from,
	dariadb::Time to,
	dariadb::Time step,
    dariadb::storage::BaseStorage_ptr ms)
{
	auto m = dariadb::Meas::empty();
	for (auto i = from; i < to; i += step) {
		m.id = id;
		m.flag = dariadb::Flag(i);
		m.time = i;
		m.value = dariadb::Value(i);
		ms->append(m);
		append_count++;
	}
}

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

void thread_writer_rnd(dariadb::Id id,
	dariadb::Time from,
	dariadb::Time to,
	dariadb::Time step,
	dariadb::storage::Capacitor *ms)
{
	auto m = dariadb::Meas::empty();
	for (auto i = from; i < to; i += step) {
		m.id = id;
		m.flag = dariadb::Flag(i);
		m.time = dariadb::timeutil::current_time() - id;
		m.value = dariadb::Value(i);
		ms->append(m);
		append_count++;
	}

}


void thread_writer_rnd_stor(dariadb::Id id,
	dariadb::Time from,
	dariadb::Time to,
	dariadb::Time step,
    dariadb::storage::BaseStorage_ptr ms)
{
	auto m = dariadb::Meas::empty();
	for (auto i = from; i < to; i += step) {
		m.id = id;
		m.flag = dariadb::Flag(i);
		m.time = dariadb::timeutil::current_time() - id;
		m.value = dariadb::Value(i);
		ms->append(m);
		append_count++;
	}

}


void show_info() {
	clock_t t0 = clock();
	auto all_writes = thread_count*iteration_count;
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
		std::vector<std::thread> writers(thread_count);
		size_t pos = 0;
		for (size_t i = 0; i < thread_count; i++) {
			std::thread t{ thread_writer, i, 0, iteration_count,1,ms };
			writers[pos++] = std::move(t);
		}
		//std::thread read_all_t{ thread_read_all, 0, dariadb::Time(iteration_count), ms };

		pos = 0;
		for (size_t i = 0; i < thread_count; i++) {
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
		std::unique_ptr<dariadb::storage::Capacitor> cp{ new dariadb::storage::Capacitor(1000, ms, 1000) };

		append_count = 0;
		stop_info = false;

		std::thread info_thread(show_info);
		std::vector<std::thread> writers(thread_count);
		size_t pos = 0;
		for (size_t i = 0; i < thread_count; i++) {
			std::thread t{ thread_writer_rnd, i, 0, iteration_count,1,cp.get() };
			writers[pos++] = std::move(t);
		}

		pos = 0;
		for (size_t i = 0; i < thread_count; i++) {
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
		const dariadb::Time old_mem_chunks = 1000;

		if (dariadb::utils::fs::path_exists(storage_path)) {
			dariadb::utils::fs::rm(storage_path);
		}

        dariadb::storage::BaseStorage_ptr ms{
			new dariadb::storage::UnionStorage(storage_path,
											   dariadb::storage::STORAGE_MODE::SINGLE,
											   chunk_per_storage,
											   chunk_size,
											   write_window_deep,
											   cap_max_size,old_mem_chunks) };

		append_count = 0;
		stop_info = false;
		stop_read_all = false;

		std::thread info_thread(show_info);
		std::vector<std::thread> writers(thread_count);

		size_t pos = 0;
		for (size_t i = 0; i < thread_count; i++) {
			std::thread t{ thread_writer_rnd_stor, i, 0, iteration_count,1,ms };
			writers[pos++] = std::move(t);
		}
		//std::thread read_all_t{ thread_read_all, 0, dariadb::Time(iteration_count), ms };

		pos = 0;
		for (size_t i = 0; i < thread_count; i++) {
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
