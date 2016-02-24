#include <ctime>
#include <iostream>
#include <cstdlib>
#include <iterator>

#include <timedb.h>
#include <ctime>
#include <limits>
#include <cmath>
int main(int argc, char *argv[]) {
    auto sz=1024*1024*100;
    uint8_t *buffer=new uint8_t[sz];

    //delta compression
    std::fill(buffer,buffer+sz,0);
    {
        timedb::compression::BinaryBuffer bw(buffer,buffer+sz);
        timedb::compression::DeltaCompressor dc(bw);

        std::vector<timedb::Time> deltas{50,255,1024,2050};
        timedb::Time t=0;
        auto start=clock();
        for(size_t i=0;i<1000000;i++){
            dc.append(t);
            t+=deltas[i%deltas.size()];
            if (t > std::numeric_limits<timedb::Time>::max()){
                t=0;
            }
        }
        auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"delta copmressor elapsed: "<<elapsed<<std::endl;
    }
    {
        timedb::compression::BinaryBuffer bw(buffer,buffer+sz);
        timedb::compression::DeltaDeCompressor dc(bw,0);

        auto start=clock();
        for(size_t i=1;i<1000000;i++){
            dc.read();
        }
        auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"delta decopmressor elapsed: "<<elapsed<<std::endl;
    }
    //xor compression
    std::fill(buffer,buffer+sz,0);
    {
        timedb::compression::BinaryBuffer bw(buffer,buffer+sz);
        timedb::compression::XorCompressor dc(bw);

        timedb::Value t=1;
        auto start=clock();
        for(size_t i=0;i<1000000;i++){
            dc.append(t);
            t*=2;
        }
        auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"xor copmressor elapsed: "<<elapsed<<std::endl;
    }
    {
        timedb::compression::BinaryBuffer bw(buffer,buffer+sz);
        timedb::compression::XorDeCompressor dc(bw,0);

        auto start=clock();
        for(size_t i=1;i<1000000;i++){
            dc.read();
        }
        auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"xor decopmressor elapsed: "<<elapsed<<std::endl;
    }
    delete[]buffer;
}
