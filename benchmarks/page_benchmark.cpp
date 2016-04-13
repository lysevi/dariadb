#include <ctime>
#include <iostream>
#include <cstdlib>
#include <iterator>

#include <dariadb.h>
#include <utils/fs.h>
#include <storage/page_manager.h>
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
const size_t chunks_count = 1024;
const size_t chunks_size = 1024;

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
        dariadb::storage::PageManager::start(dariadb::storage::PageManager::Params(storagePath, dariadb::storage::MODE::SINGLE, chunks_count, chunks_size));

		auto start = clock();

		for (size_t i = 0; i < chunks_count; ++i) {
			dariadb::storage::PageManager::instance()->append_chunk(ch);
			ch->maxTime += dariadb::Time(chunks_size);
			ch->minTime += dariadb::Time(chunks_size);
		}

		auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
		std::cout << "insert : " << elapsed << std::endl;

		BenchCallback*clbk = new BenchCallback;
		start = clock();

		dariadb::Time to_t = chunks_size;
		for (size_t i = 1; i < chunks_count; i++) {
			auto cursor = dariadb::storage::PageManager::instance()->chunksByIterval(dariadb::IdArray{}, 0, to_t, 0);
			cursor->readAll(clbk);
			cursor = nullptr;
			to_t *= 2;
		}

		elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
		std::cout << "readAll : " << elapsed << std::endl;
		
		delete clbk;
		dariadb::storage::PageManager::stop();
		if (dariadb::utils::fs::path_exists(storagePath)) {
			dariadb::utils::fs::rm(storagePath);
		}
		ch = nullptr;
	}
}
