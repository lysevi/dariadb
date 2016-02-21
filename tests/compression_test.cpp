#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include <compression.h>

#include <iterator>
#include <sstream>
#include<iostream>
class Mok_DeltaCompressor:public timedb::compression::DeltaCompressor{
public:
    Mok_DeltaCompressor(const timedb::compression::BinaryWriter&buf):timedb::compression::DeltaCompressor(buf)
    {}
    uint64_t get_prev_delta()const {return this->_prev_delta;}
    timedb::Time get_prev_time()const {return this->_prev_time;}
    timedb::Time  get_first()const {return this->_first;}
    timedb::compression::BinaryWriter get_bw()const{return this->_bw;}
    bool is_first()const {return this->_is_first;}
};

class Mok_DeltaDeCompressor:public timedb::compression::DeltaDeCompressor{
public:
    Mok_DeltaDeCompressor(const timedb::compression::BinaryWriter&buf,  timedb::Time first):timedb::compression::DeltaDeCompressor(buf,first)
    {}
    uint64_t get_prev_delta()const {return this->_prev_delta;}
    timedb::Time get_prev_time()const {return this->_prev_time;}
    timedb::compression::BinaryWriter get_bw()const{return this->_bw;}
};

BOOST_AUTO_TEST_CASE(binary_writer) {
	const size_t buffer_size = 10;
	const size_t writed_bits = 7 * buffer_size;
    uint8_t buffer[buffer_size];
	//check ctor
    timedb::compression::BinaryWriter b(std::begin(buffer),std::end(buffer));
	BOOST_CHECK_EQUAL(b.cap(), buffer_size);

	BOOST_CHECK_EQUAL(b.bitnum(), 7);
    BOOST_CHECK_EQUAL(b.pos(), size_t(0));
	
	//check incs work fine
	b.incbit();
	BOOST_CHECK_EQUAL(b.bitnum(), 6);
    BOOST_CHECK_EQUAL(b.pos(), size_t(0));

	b.incbit(); b.incbit(); b.incbit(); 
	b.incbit(); b.incbit(); b.incbit();
	b.incbit();
	BOOST_CHECK_EQUAL(b.bitnum(), 7);
    BOOST_CHECK_EQUAL(b.pos(), size_t(1));

	
	{//ctors test.
		timedb::compression::BinaryWriter copy_b(b);
		BOOST_CHECK_EQUAL(b.bitnum(), copy_b.bitnum());
		BOOST_CHECK_EQUAL(b.pos(), copy_b.pos());
		auto move_b = std::move(copy_b);
        BOOST_CHECK(size_t(copy_b.bitnum()) == copy_b.pos() && size_t(copy_b.bitnum()) == copy_b.cap());
        BOOST_CHECK_EQUAL(copy_b.bitnum(),0);

		BOOST_CHECK_EQUAL(move_b.bitnum(), b.bitnum());
		BOOST_CHECK_EQUAL(move_b.pos(), b.pos());
		BOOST_CHECK_EQUAL(move_b.cap(), b.cap());
	}
	// set/clr bit 
	b.reset_pos();
	// write 101010101...
	for (size_t i = 0; i <writed_bits; i++) {
		if (i % 2) {
            b.setbit().incbit();
		}
		else {
            b.clrbit().incbit();
		}
	}

	b.reset_pos();

	for (size_t i = 0; i < writed_bits; i++) {
		if (i % 2) {
			BOOST_CHECK_EQUAL(b.getbit(), uint8_t(1));
		}
		else {
			BOOST_CHECK_EQUAL(b.getbit(), uint8_t(0));
		}
		b.incbit();
	}

    std::stringstream ss{};
    ss<<b;
    BOOST_CHECK(ss.str().size()!=0);

	b.reset_pos();
	// clear all bits
	for (size_t i = 0; i < writed_bits; i++) {
		b.clrbit();
		BOOST_CHECK_EQUAL(b.getbit(), uint8_t(0));
		b.incbit();
	}
}


BOOST_AUTO_TEST_CASE(DeltaCompressor_deltas) {
    BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_64(1), 513);
    BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_64(64), 576);
    BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_64(63), 575);

    BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_256(256), 3328);
    BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_256(255), 3327);
    BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_256(65), 3137);

    BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_2048(2048), 59392);
    BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_2048(257), 57601);
    BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_2048(1500), 58844);

    BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_big(2049), uint64_t(64424511489));
    BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_big(65535),uint64_t( 64424574975));
    BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_big(4095), uint64_t(64424513535));
    BOOST_CHECK_EQUAL(timedb::compression::DeltaCompressor::get_delta_big(4294967295), uint64_t(68719476735));
}

BOOST_AUTO_TEST_CASE(DeltaCompressor){
    const size_t test_buffer_size =100;

    const timedb::Time t1=100;
    const timedb::Time t2=150;
    const timedb::Time t3=200;
    const timedb::Time t4=2000;
    const timedb::Time t5=3000;

    uint8_t buffer[test_buffer_size];
    std::fill(std::begin(buffer),std::end(buffer),0);
    {
        Mok_DeltaCompressor dc(timedb::compression::BinaryWriter(std::begin(buffer),std::end(buffer)));
        BOOST_CHECK(dc.is_first());


        dc.append(t1);
        BOOST_CHECK(!dc.is_first());
        BOOST_CHECK_EQUAL(dc.get_first(),t1);
        BOOST_CHECK_EQUAL(dc.get_prev_time(),t1);

        dc.append(t1);
        BOOST_CHECK_EQUAL(dc.get_bw().bitnum(),timedb::compression::max_bit_pos-1);
        BOOST_CHECK_EQUAL(buffer[0],0);
    }
    {
         std::fill(std::begin(buffer),std::end(buffer),0);
         Mok_DeltaCompressor dc(timedb::compression::BinaryWriter(std::begin(buffer),std::end(buffer)));

         dc.append(t1);
         dc.append(t2);
         BOOST_CHECK_EQUAL(dc.get_prev_time(),t2);
         BOOST_CHECK_EQUAL(dc.get_prev_delta(),t2-t1);
         BOOST_CHECK_EQUAL(int(buffer[0]),int(140));
         BOOST_CHECK_EQUAL(int(buffer[1]),int(128));
    }
    {
         std::fill(std::begin(buffer),std::end(buffer),0);
         Mok_DeltaCompressor dc(timedb::compression::BinaryWriter(std::begin(buffer),std::end(buffer)));

         dc.append(t1);
         dc.append(t3);
         BOOST_CHECK_EQUAL(dc.get_prev_time(),t3);
         BOOST_CHECK_EQUAL(dc.get_prev_delta(),t3-t1);
         BOOST_CHECK_EQUAL(int(buffer[0]),int(198));
         BOOST_CHECK_EQUAL(int(buffer[1]),int(64));
    }

    {
         std::fill(std::begin(buffer),std::end(buffer),0);
         Mok_DeltaCompressor dc(timedb::compression::BinaryWriter(std::begin(buffer),std::end(buffer)));

         dc.append(t1);
         dc.append(t4);
         BOOST_CHECK_EQUAL(dc.get_prev_time(),t4);
         BOOST_CHECK_EQUAL(dc.get_prev_delta(),t4-t1);
         BOOST_CHECK_EQUAL(int(buffer[0]),int(231));
         BOOST_CHECK_EQUAL(int(buffer[1]),int(108));
    }
    {
         std::fill(std::begin(buffer),std::end(buffer),0);
         Mok_DeltaCompressor dc(timedb::compression::BinaryWriter(std::begin(buffer),std::end(buffer)));

         dc.append(t1);
         dc.append(t5);
         BOOST_CHECK_EQUAL(dc.get_prev_time(),t5);
         BOOST_CHECK_EQUAL(dc.get_prev_delta(),t5-t1);
         BOOST_CHECK_EQUAL(int(buffer[0]),int(240));
         BOOST_CHECK_EQUAL(int(buffer[1]),int(0));
         BOOST_CHECK_EQUAL(int(buffer[2]),int(0));
         BOOST_CHECK_EQUAL(int(buffer[3]),int(181));
         BOOST_CHECK_EQUAL(int(buffer[4]),int(64));
    }
}

BOOST_AUTO_TEST_CASE(DeltaDeCompressor){
    const size_t test_buffer_size =1000;
    const timedb::Time t1=100;
    const timedb::Time t2=150;
    const timedb::Time t3=200;
    const timedb::Time t4=t3+100;
    const timedb::Time t5=t4+1000;
    const timedb::Time t6=t5*20;
    const timedb::Time t7=t6-50;

    uint8_t buffer[test_buffer_size];
    std::fill(std::begin(buffer),std::end(buffer),0);
    {
        Mok_DeltaCompressor co(timedb::compression::BinaryWriter(std::begin(buffer),std::end(buffer)));

        co.append(t1);
        co.append(t2);
        co.append(t3);
        co.append(t4);
        co.append(t5);
        co.append(t6);
        co.append(t7);
        Mok_DeltaDeCompressor dc(timedb::compression::BinaryWriter(std::begin(buffer),std::end(buffer)),t1);
        BOOST_CHECK_EQUAL(dc.read(),t2);
        BOOST_CHECK_EQUAL(dc.read(),t3);
        BOOST_CHECK_EQUAL(dc.read(),t4);
        BOOST_CHECK_EQUAL(dc.read(),t5);
        BOOST_CHECK_EQUAL(dc.read(),t6);
        BOOST_CHECK_EQUAL(dc.read(),t7);
    }

    std::fill(std::begin(buffer),std::end(buffer),0);
    {
        Mok_DeltaCompressor co(timedb::compression::BinaryWriter(std::begin(buffer),std::end(buffer)));
        timedb::Time delta=1;
        std::list<timedb::Time> times{};
        const int steps=30;
        for (int i=0;i<steps;i++){
            co.append(delta);
            times.push_back(delta);
            delta*=2;
        }

        Mok_DeltaDeCompressor dc(timedb::compression::BinaryWriter(std::begin(buffer),std::end(buffer)),times.front());
        times.pop_front();
        for(auto&t:times){
            auto readed=dc.read();
            BOOST_CHECK_EQUAL(readed,t);
        }
    }

}
