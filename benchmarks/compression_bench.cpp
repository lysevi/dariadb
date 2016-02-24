#include <ctime>
#include <iostream>
#include <cstdlib>
#include <iterator>

#include <timedb.h>
#include <ctime>
#include <limits>
#include <cmath>
int main(int argc, char *argv[]) {
    auto test_buffer_size=1024*1024*100;
    uint8_t *buffer=new uint8_t[test_buffer_size];

    //delta compression
    std::fill(buffer,buffer+test_buffer_size,0);
    {
        timedb::compression::BinaryBuffer bw(buffer,buffer+test_buffer_size);
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
        std::cout<<"delta copmressor : "<<elapsed<<std::endl;
    }
    {
        timedb::compression::BinaryBuffer bw(buffer,buffer+test_buffer_size);
        timedb::compression::DeltaDeCompressor dc(bw,0);

        auto start=clock();
        for(size_t i=1;i<1000000;i++){
            dc.read();
        }
        auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"delta decopmressor : "<<elapsed<<std::endl;
    }
    //xor compression
    std::fill(buffer,buffer+test_buffer_size,0);
    {
        timedb::compression::BinaryBuffer bw(buffer,buffer+test_buffer_size);
        timedb::compression::XorCompressor dc(bw);

        timedb::Value t=1;
        auto start=clock();
        for(size_t i=0;i<1000000;i++){
            dc.append(t);
            t*=2;
        }
        auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"xor copmressor : "<<elapsed<<std::endl;
    }
    {
        timedb::compression::BinaryBuffer bw(buffer,buffer+test_buffer_size);
        timedb::compression::XorDeCompressor dc(bw,0);

        auto start=clock();
        for(size_t i=1;i<1000000;i++){
            dc.read();
        }
        auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"xor decopmressor : "<<elapsed<<std::endl;
    }
	{
		uint8_t* time_begin=new uint8_t[test_buffer_size];
		auto time_end = time_begin + test_buffer_size;

		uint8_t* value_begin = new uint8_t[test_buffer_size];
		auto value_end = value_begin + test_buffer_size;

		uint8_t* flag_begin = new uint8_t[test_buffer_size];
		auto flag_end = flag_begin + test_buffer_size;

		std::fill(time_begin, time_end, 0);
		std::fill(flag_begin, flag_end, 0);
		std::fill(value_begin, value_end, 0);

		timedb::compression::CopmressedWriter cwr(timedb::compression::BinaryBuffer(time_begin, time_end),
			timedb::compression::BinaryBuffer(value_begin, value_end),
			timedb::compression::BinaryBuffer(flag_begin, flag_end));
		auto start = clock();
		for (int i = 0; i < 1000000; i++) {
			auto m = timedb::Meas::empty();
			m.time = i;
			m.flag = i;
			m.value = i;
			cwr.append(m);
		}

		auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
		std::cout << "compress writer : " << elapsed << std::endl;

		auto m = timedb::Meas::empty();

		timedb::compression::CopmressedReader crr(timedb::compression::BinaryBuffer(time_begin, time_end),
			timedb::compression::BinaryBuffer(value_begin, value_end),
			timedb::compression::BinaryBuffer(flag_begin, flag_end),m);

		start = clock();
		for (int i = 1; i < 1000000; i++) {
			crr.read();
		}
		elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
		std::cout << "compress reader : " << elapsed << std::endl;

		delete[] time_begin;
		delete[] value_begin;
		delete[] flag_begin;
	}
    delete[]buffer;
}
