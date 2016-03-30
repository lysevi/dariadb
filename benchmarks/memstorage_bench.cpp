#include <ctime>
#include <iostream>
#include <cstdlib>
#include <iterator>

#include <dariadb.h>
#include <storage/memstorage.h>
#include <ctime>
#include <limits>
#include <cmath>
#include <chrono>

class BenchCallback:public dariadb::storage::ReaderClb{
public:
    void call(const dariadb::Meas&){
        count++;
    }
    size_t count;
};


int main(int argc, char *argv[]) {
	(void)argc;
	(void)argv;
	{
		auto ms = new dariadb::storage::MemoryStorage{ 512 };
		auto m = dariadb::Meas::empty();

		std::vector<dariadb::Time> deltas{ 50,255,1024,2050 };
		auto now = std::chrono::system_clock::now();
		dariadb::Time t = dariadb::timeutil::from_chrono(now);
		const size_t ids_count = 2;

		auto start = clock();

		const size_t K = 1;
		
		for (size_t i = 0; i < K * 1000000; i++) {
			m.id = i%ids_count;
			m.flag = 0xff;
			t += deltas[i%deltas.size()];
			m.time = t;
			m.value = dariadb::Value(i);
			ms->append(m);
		}

		auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
		std::cout << "memorystorage insert : " << elapsed << std::endl;
		auto clbk = new BenchCallback();
		clbk->count = 0;

		start = clock();
		auto reader = ms->readInTimePoint(ms->maxTime());

		reader->readAll(clbk);

		elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
		std::cout << "memorystorage readTimePoint last: " << elapsed << std::endl;
		std::cout << "raded: " << clbk->count << std::endl;

		start = clock();
		auto reader_int = ms->readInterval(dariadb::timeutil::from_chrono(now), t);

		clbk->count = 0;
		reader_int->readAll(clbk);

		elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
		std::cout << "memorystorage readInterval all: " << elapsed << std::endl;
		std::cout << "raded: " << clbk->count << std::endl;


		start = clock();
		auto reader_by_step = ms->readInterval(dariadb::timeutil::from_chrono(now), t);

		clbk->count = 0;
		dariadb::Time query_step = 100000;
		reader_by_step->readByStep(clbk, dariadb::timeutil::from_chrono(now) - query_step * 10, t, query_step);

		elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
		std::cout << "memorystorage byStep(" << query_step << ") all: " << elapsed << std::endl;
		std::cout << "raded: " << clbk->count << std::endl;
		delete ms;
	}
}
