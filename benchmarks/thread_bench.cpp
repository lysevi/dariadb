#include <ctime>
#include <iostream>
#include <cstdlib>
#include <iterator>

#include <dariadb.h>
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

std::atomic_long append_count{ 0 };
size_t thread_count = 5;
size_t iteration_count = 1000000;
bool stop_info = false;

void thread_writer(dariadb::Id id,
	dariadb::Time from,
	dariadb::Time to,
	dariadb::Time step,
	dariadb::storage::AbstractStorage_ptr ms)
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
		m.time = dariadb::timeutil::current_time()-id;
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
		std::cout << "\rwrites: " << writes_per_sec << "/sec progress:" << (100 * append_count) / all_writes << "%             ";
		std::cout.flush();
		if (stop_info) {
			break;
		}
	}
	std::cout << "\n";
}

int main(int argc, char *argv[]) {
	(void)argc;
	(void)argv;
	{
		dariadb::storage::AbstractStorage_ptr ms{ new dariadb::storage::MemoryStorage{ 2000000 } };
		std::thread info_thread(show_info);
		std::vector<std::thread> writers(thread_count);
		size_t pos = 0;
		for (size_t i = 0; i < thread_count; i++) {
			std::thread t{ thread_writer, i, 0, iteration_count,1,ms };
			writers[pos++] = std::move(t);
		}

		pos = 0;
		for (size_t i = 0; i < thread_count; i++) {
			std::thread t = std::move(writers[pos++]);
			t.join();
		}
		stop_info = true;
		info_thread.join();
	}
	{
		std::cout << "Capacitor" << std::endl;
		dariadb::storage::AbstractStorage_ptr ms{ new dariadb::storage::MemoryStorage{ 2000000 } };
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
}
