#include <ctime>
#include <iostream>
#include <cstdlib>
#include <iterator>

#include <dariadb.h>
#include <compression.h>
#include <compression/delta.h>
#include <compression/xor.h>
#include <compression/flag.h>
#include <ctime>
#include <limits>
#include <cmath>
#include <chrono>

int main(int argc, char *argv[]) {
	(void)argc;
	(void)argv;

    auto test_buffer_size=1024*1024*100;
    uint8_t *buffer=new uint8_t[test_buffer_size];

    //delta compression
    std::fill(buffer,buffer+test_buffer_size,0);
    {
        const size_t count=1000000;
        dariadb::compression::BinaryBuffer bw({buffer,buffer+test_buffer_size});
        dariadb::compression::DeltaCompressor dc(bw);

        std::vector<dariadb::Time> deltas{50,255,1024,2050};
        dariadb::Time t=0;
        auto start=clock();
        for(size_t i=0;i<count;i++){
            dc.append(t);
            t+=deltas[i%deltas.size()];
            if (t > std::numeric_limits<dariadb::Time>::max()){
                t=0;
            }
        }
        auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"delta compressor : "<<elapsed<<std::endl;

        auto w=dc.used_space();
        auto sz=sizeof(dariadb::Time)*count;
        std::cout<<"used space:  "
               <<(w*100.0)/(sz)<<"%"
               <<std::endl;
    }
    {
        dariadb::compression::BinaryBuffer bw({buffer,buffer+test_buffer_size});
        dariadb::compression::DeltaDeCompressor dc(bw,0);

        auto start=clock();
        for(size_t i=1;i<1000000;i++){
            dc.read();
        }
        auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"delta decompressor : "<<elapsed<<std::endl;
    }
    //xor compression
    std::fill(buffer,buffer+test_buffer_size,0);
    {
        const size_t count=1000000;
        dariadb::compression::BinaryBuffer bw({buffer,buffer+test_buffer_size});
        dariadb::compression::XorCompressor dc(bw);

        dariadb::Value t=3.14;
        auto start=clock();
        for(size_t i=0;i<count;i++){
            dc.append(t);
            t*=1.5;
        }
        auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"\nxor compressor : "<<elapsed<<std::endl;
        auto w=dc.used_space();
        auto sz=sizeof(dariadb::Time)*count;
        std::cout<<"used space: "
               <<(w*100.0)/(sz)<<"%"
               <<std::endl;
    }
    {
        dariadb::compression::BinaryBuffer bw({buffer,buffer+test_buffer_size});
        dariadb::compression::XorDeCompressor dc(bw,0);

        auto start=clock();
        for(size_t i=1;i<1000000;i++){
            dc.read();
        }
        auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"xor decompressor : "<<elapsed<<std::endl;
    }


    {
        const size_t count = 1000000;
        auto start = clock();
        for (size_t i = 0; i<count; i++) {
            dariadb::compression::inner::flat_double_to_int(3.14);
        }
        auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
        std::cout << "\nflat_double_to_int: " << elapsed << std::endl;

        start = clock();
        for (size_t i = 0; i<count; i++) {
            dariadb::compression::inner::flat_int_to_double(0xfff);
        }
        elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
        std::cout << "flat_int_to_double: " << elapsed << std::endl;
    }

    {
        const size_t count=1000000;
        uint8_t* time_begin=new uint8_t[test_buffer_size];
        auto time_r = dariadb::utils::Range{time_begin, time_begin + test_buffer_size};

        uint8_t* value_begin = new uint8_t[test_buffer_size];
        auto value_r = dariadb::utils::Range{value_begin,value_begin + test_buffer_size};

        uint8_t* flag_begin = new uint8_t[test_buffer_size];
        auto flag_r = dariadb::utils::Range{flag_begin,flag_begin + test_buffer_size};

        std::fill(time_r.begin, time_r.end, 0);
        std::fill(time_r.begin, time_r.end, 0);
        std::fill(time_r.begin, time_r.end, 0);

        dariadb::compression::CopmressedWriter cwr{dariadb::compression::BinaryBuffer(time_r),
                                                     dariadb::compression::BinaryBuffer(value_r),
                                                     dariadb::compression::BinaryBuffer(flag_r)};
        auto start = clock();
        for (size_t i = 0; i < count; i++) {
            auto m = dariadb::Meas::empty();
            m.time = static_cast<dariadb::Time>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count());
            m.flag = i;
            m.value = i;
            cwr.append(m);
        }

        auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
        std::cout << "\ncompress writer : " << elapsed << std::endl;
        auto w=cwr.used_space();
        auto sz=sizeof(dariadb::Meas)*count;
        std::cout<<"used space: "
               <<(w*100.0)/(sz)<<"%"
               <<std::endl;

        auto m = dariadb::Meas::empty();

        dariadb::compression::CopmressedReader crr{dariadb::compression::BinaryBuffer(time_r),
                    dariadb::compression::BinaryBuffer(value_r),
                    dariadb::compression::BinaryBuffer(flag_r),m};

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
