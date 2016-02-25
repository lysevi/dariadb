#include <ctime>
#include <iostream>
#include <cstdlib>
#include <iterator>

#include <timedb.h>
#include <ctime>
#include <limits>
#include <cmath>

#include <timedb.h>

int main(int argc, char *argv[]) {
    {
		auto ms = new timedb::storage::MemoryStorage{ 100000 };
		auto m = timedb::Meas::empty();
		
		std::vector<timedb::Time> deltas{ 50,255,1024,2050 };
		timedb::Time t = 1;
		const size_t ids_count = 10;
        auto start=clock();
		for (auto i = 0; i < 1000000; i ++) {
			m.id = i%10;
			m.flag = 0xff;
			m.time=t+deltas[i%deltas.size()];
			auto sv = (std::sin(i));
			m.value = *(timedb::Id*)(&sv);
			ms->append(m);
		}
        auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"memorystorage insert : "<<elapsed<<std::endl;
		delete ms;
    }
}
