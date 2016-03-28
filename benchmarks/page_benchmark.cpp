#include <ctime>
#include <iostream>
#include <cstdlib>
#include <iterator>

#include <dariadb.h>
#include <storage/fs.h>
#include <ctime>
#include <limits>
#include <cmath>
#include <chrono>
#include <thread>
#include <atomic>
#include <cassert>

class BenchCallback :public dariadb::storage::Cursor::Callback {
public:
	void call(dariadb::storage::Chunk_Ptr &) {
		count++;
	}
	size_t count;
};

const std::string storagePath = "benchStorage/";
std::atomic_long append_count{ 0 }, read_all_times{ 0 };
size_t thread_count = 5;
size_t iteration_count = 1000000;
const size_t chunks_count = 1024;
const size_t chunks_size = 1024;
bool stop_info = false;
bool stop_read_all{ false };


int main(int argc, char *argv[]) {
	(void)argc;
	(void)argv;
	{
		dariadb::Time t = 0;

		dariadb::Meas first;
		first.id = 1;
		first.time = t;

		dariadb::storage::Chunk_Ptr ch = std::make_shared<dariadb::storage::Chunk>(chunks_size, first);
		for (int i = 0;; i++, t++) {
			first.flag = dariadb::Flag(i);
			first.time = t;
			first.value = dariadb::Value(i);
			if (!ch->append(first)) {
				break;
			}
		}
		if (dariadb::utils::fs::path_exists(storagePath)) {
			dariadb::utils::fs::rm(storagePath);
		}
		dariadb::storage::PageManager::start(storagePath, dariadb::storage::STORAGE_MODE::SINGLE, chunks_count, chunks_size);

		auto start = clock();

		for (size_t i = 0; i < chunks_count; ++i) {
			auto res = dariadb::storage::PageManager::instance()->append_chunk(ch);
			ch->maxTime += dariadb::Time(chunks_size);
			ch->minTime += dariadb::Time(chunks_size);

			assert(res);
		}

		auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
		std::cout << "insert : " << elapsed << std::endl;

		dariadb::storage::PageManager::stop();
		if (dariadb::utils::fs::path_exists(storagePath)) {
			dariadb::utils::fs::rm(storagePath);
		}
	}
}
