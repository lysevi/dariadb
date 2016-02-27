#include <ctime>
#include <iostream>
#include <cstdlib>
#include <iterator>

#include <memseries.h>
#include <ctime>
#include <limits>
#include <cmath>
#include <chrono>
int main(int argc, char *argv[]) {
    {
		auto ms = new memseries::storage::MemoryStorage{ 1000000 };
		auto m = memseries::Meas::empty();

		std::vector<memseries::Time> deltas{ 50,255,1024,2050 };
        auto now=std::chrono::high_resolution_clock::now();
        memseries::Time t = memseries::timeutil::from_chrono(now);
		const size_t ids_count = 2;

		auto start = clock();
		
        const size_t K = 2;
        for (size_t i = 0; i < K*1000000; i++) {
			m.id = i%ids_count;
			m.flag = 0xff;
            t += deltas[i%deltas.size()];
            m.time = t;
			m.value = i;
			ms->append(m);
		}

        auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"memorystorage insert : "<<elapsed<<std::endl;

        start = clock();
        auto reader=ms->readInTimePoint(ms->maxTime());

        memseries::Meas::MeasList mlist{};
        reader->readAll(&mlist);

        elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"memorystorage read: "<<elapsed<<std::endl;
        std::cout<<"raded: "<<mlist.size()<<std::endl;
        delete ms;
    }
}
