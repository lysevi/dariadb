#include <ctime>
#include <iostream>
#include <cstdlib>
#include <iterator>

#include <time_ordered_set.h>
#include <membucket.h>
#include <ctime>

int main(int argc, char *argv[]) {
    const size_t K = 1;

    {
        const size_t max_size=K*100000;
        auto tos =memseries::storage::TimeOrderedSet{ max_size };
        auto m = memseries::Meas::empty();

        auto start = clock();


        for (size_t i = 0; i < max_size; i++) {
            m.id = 1;
            m.flag = 0xff;
            m.time = i;
            m.value = i;
            tos.append(m);
        }

        auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"TimeOrderedSet insert : "<<elapsed<<std::endl;

        start = clock();
        auto reader = tos.as_array();

        elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
        std::cout << "TimeOrderedSet as_array: " << elapsed << std::endl;
        std::cout << "raded: " << reader.size() << std::endl;
    }

    {
        const size_t max_size=100000;
        const size_t max_count=K*10;
        auto tos =memseries::storage::MemBucket{ max_size,max_count };
        auto m = memseries::Meas::empty();

        auto start = clock();


        for (size_t i = 0; i < max_size; i++) {
            m.id = 1;
            m.flag = 0xff;
            m.time = i;
            m.value = i;
            tos.append(m);
        }

        auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"MemBucket insert : "<<elapsed<<std::endl;
    }
}
