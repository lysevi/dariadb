#include <ctime>
#include <iostream>
#include <cstdlib>
#include <iterator>

#include <timedb.h>
#include <ctime>
#include <limits.h>
int main(int argc, char *argv[]) {
    auto sz=1024*1024*100;
    uint8_t *buffer=new uint8_t[sz];

    std::fill(buffer,buffer+sz,0);

    timedb::compression::BinaryWriter bw(buffer,buffer+sz);
    timedb::compression::DeltaCompressor dc(bw);

    std::vector<timedb::Time> deltas{50,255,1024,2050};
    timedb::Time t=0;
    auto start=clock();
    for(size_t i=0;i<1000000;i++){
        dc.append(t);
        t+=deltas[i%deltas.size()];
        if (t> _UI32_MAX){// int32
            t=0;
        }
    }
    auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
    std::cout<<"elapsed: "<<elapsed<<std::endl;
    delete[]buffer;
}
