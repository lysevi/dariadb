#include <ctime>
#include <iostream>
#include <cstdlib>
#include <iterator>

#include <memseries.h>
#include <ctime>
#include <limits>
#include <cmath>

#include <memseries.h>

int main(int argc, char *argv[]) {
    {
		auto ms = new memseries::storage::MemoryStorage{ 100000 };
		auto m = memseries::Meas::empty();

		std::vector<memseries::Time> deltas{ 50,255,1024,2050 };
		memseries::Time t = 1;
		const size_t ids_count = 10;

		auto start = clock();
		for (int K = 0; K < 5; K++) {
			
			for (auto i = 0; i < 1000000; i++) {
				m.id = i%ids_count;
				m.flag = 0xff;
				m.time = t + deltas[i%deltas.size()];
				m.value = K+i/10.0;
				ms->append(m);
			}
		}
        auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"memorystorage insert : "<<elapsed<<std::endl;
		delete ms;
    }
}
