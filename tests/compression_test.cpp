#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include <compression.h>
#include <compression/delta.h>
#include <compression/xor.h>
#include <compression/flag.h>

#include <iterator>
#include <sstream>
#include <chrono>

class Testable_DeltaCompressor:public memseries::compression::DeltaCompressor{
public:
    Testable_DeltaCompressor(const memseries::compression::BinaryBuffer&buf):memseries::compression::DeltaCompressor(buf)
    {}
    uint64_t get_prev_delta()const {return this->_prev_delta;}
    memseries::Time get_prev_time()const {return this->_prev_time;}
    memseries::Time  get_first()const {return this->_first;}
    memseries::compression::BinaryBuffer get_bw()const{return this->_bw;}
    bool is_first()const {return this->_is_first;}
};

class Testable_DeltaDeCompressor:public memseries::compression::DeltaDeCompressor{
public:
    Testable_DeltaDeCompressor(const memseries::compression::BinaryBuffer&buf,  memseries::Time first):memseries::compression::DeltaDeCompressor(buf,first)
    {}
    uint64_t get_prev_delta()const {return this->_prev_delta;}
    memseries::Time get_prev_time()const {return this->_prev_time;}
    memseries::compression::BinaryBuffer get_bw()const{return this->_bw;}
};

class Testable_XorCompressor:public memseries::compression::XorCompressor{
public:
    Testable_XorCompressor(const memseries::compression::BinaryBuffer&buf):memseries::compression::XorCompressor(buf)
    {}
    memseries::compression::BinaryBuffer get_bw()const{return this->_bw;}
    memseries::Value get_prev_value()const {return  memseries::compression::inner::flat_int_to_double(this->_prev_value);}
    memseries::Value get_first()const {return memseries::compression::inner::flat_int_to_double(this->_first);}
    bool is_first()const {return this->_is_first;}
    void set_is_first(bool flag) {this->_is_first=flag;}
    uint8_t  get_prev_lead()const{return  _prev_lead;}
    uint8_t  get_prev_tail()const{return  _prev_tail;}
    void  set_prev_lead(uint8_t v){_prev_lead=v;}
    void  set_prev_tail(uint8_t v){_prev_tail=v;}
};

BOOST_AUTO_TEST_CASE(binary_writer) {
	const size_t buffer_size = 10;
	const size_t writed_bits = 7 * buffer_size;
    uint8_t buffer[buffer_size];
	//check ctor
    memseries::compression::BinaryBuffer b(std::begin(buffer),std::end(buffer));
	BOOST_CHECK_EQUAL(b.cap(), buffer_size);

	BOOST_CHECK_EQUAL(b.bitnum(), 7);
    BOOST_CHECK_EQUAL(b.pos(), size_t(buffer_size-1));
	
	//check incs work fine
	b.incbit();
	BOOST_CHECK_EQUAL(b.bitnum(), 6);
    BOOST_CHECK_EQUAL(b.pos(), size_t(buffer_size-1));

	b.incbit(); b.incbit(); b.incbit(); 
	b.incbit(); b.incbit(); b.incbit();
	b.incbit();
	BOOST_CHECK_EQUAL(b.bitnum(), 7);
    BOOST_CHECK_EQUAL(b.pos(), size_t(buffer_size-2));

	
	{//ctors test.
		memseries::compression::BinaryBuffer copy_b(b);
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
    BOOST_CHECK_EQUAL(memseries::compression::DeltaCompressor::get_delta_64(1), 513);
    BOOST_CHECK_EQUAL(memseries::compression::DeltaCompressor::get_delta_64(64), 576);
    BOOST_CHECK_EQUAL(memseries::compression::DeltaCompressor::get_delta_64(63), 575);

    BOOST_CHECK_EQUAL(memseries::compression::DeltaCompressor::get_delta_256(256), 3328);
    BOOST_CHECK_EQUAL(memseries::compression::DeltaCompressor::get_delta_256(255), 3327);
    BOOST_CHECK_EQUAL(memseries::compression::DeltaCompressor::get_delta_256(65), 3137);

    BOOST_CHECK_EQUAL(memseries::compression::DeltaCompressor::get_delta_2048(2048), 59392);
    BOOST_CHECK_EQUAL(memseries::compression::DeltaCompressor::get_delta_2048(257), 57601);
    BOOST_CHECK_EQUAL(memseries::compression::DeltaCompressor::get_delta_2048(1500), 58844);

    BOOST_CHECK_EQUAL(memseries::compression::DeltaCompressor::get_delta_big(2049), uint64_t(64424511489));
    BOOST_CHECK_EQUAL(memseries::compression::DeltaCompressor::get_delta_big(65535),uint64_t( 64424574975));
    BOOST_CHECK_EQUAL(memseries::compression::DeltaCompressor::get_delta_big(4095), uint64_t(64424513535));
    BOOST_CHECK_EQUAL(memseries::compression::DeltaCompressor::get_delta_big(4294967295), uint64_t(68719476735));
}

BOOST_AUTO_TEST_CASE(DeltaCompressor){
    const size_t test_buffer_size =100;

    const memseries::Time t1=100;
    const memseries::Time t2=150;
    const memseries::Time t3=200;
    const memseries::Time t4=2000;
    const memseries::Time t5=3000;

    uint8_t buffer[test_buffer_size];
    std::fill(std::begin(buffer),std::end(buffer),0);
    {
        Testable_DeltaCompressor dc(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)));
        BOOST_CHECK(dc.is_first());


        dc.append(t1);
        BOOST_CHECK(!dc.is_first());
        BOOST_CHECK_EQUAL(dc.get_first(),t1);
        BOOST_CHECK_EQUAL(dc.get_prev_time(),t1);

        dc.append(t1);
        BOOST_CHECK_EQUAL(dc.get_bw().bitnum(),memseries::compression::max_bit_pos-1);
        BOOST_CHECK_EQUAL(buffer[0],0);
    }
    {
         std::fill(std::begin(buffer),std::end(buffer),0);
         Testable_DeltaCompressor dc(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)));

         dc.append(t1);
         dc.append(t2);
         BOOST_CHECK_EQUAL(dc.get_prev_time(),t2);
         BOOST_CHECK_EQUAL(dc.get_prev_delta(),t2-t1);
         BOOST_CHECK_EQUAL(int(buffer[test_buffer_size-1]),int(140));
         BOOST_CHECK_EQUAL(int(buffer[test_buffer_size-2]),int(128));
    }
    {
         std::fill(std::begin(buffer),std::end(buffer),0);
         Testable_DeltaCompressor dc(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)));

         dc.append(t1);
         dc.append(t3);
         BOOST_CHECK_EQUAL(dc.get_prev_time(),t3);
         BOOST_CHECK_EQUAL(dc.get_prev_delta(),t3-t1);
         BOOST_CHECK_EQUAL(int(buffer[test_buffer_size-1]),int(198));
         BOOST_CHECK_EQUAL(int(buffer[test_buffer_size-2]),int(64));
    }

    {
         std::fill(std::begin(buffer),std::end(buffer),0);
         Testable_DeltaCompressor dc(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)));

         dc.append(t1);
         dc.append(t4);
         BOOST_CHECK_EQUAL(dc.get_prev_time(),t4);
         BOOST_CHECK_EQUAL(dc.get_prev_delta(),t4-t1);
         BOOST_CHECK_EQUAL(int(buffer[test_buffer_size-1]),int(231));
         BOOST_CHECK_EQUAL(int(buffer[test_buffer_size-2]),int(108));
    }
    {
         std::fill(std::begin(buffer),std::end(buffer),0);
         Testable_DeltaCompressor dc(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)));

         dc.append(t1);
         dc.append(t5);
         BOOST_CHECK_EQUAL(dc.get_prev_time(),t5);
         BOOST_CHECK_EQUAL(dc.get_prev_delta(),t5-t1);
         BOOST_CHECK_EQUAL(int(buffer[test_buffer_size-1]),int(240));
         BOOST_CHECK_EQUAL(int(buffer[test_buffer_size-2]),int(0));
         BOOST_CHECK_EQUAL(int(buffer[test_buffer_size-3]),int(0));
         BOOST_CHECK_EQUAL(int(buffer[test_buffer_size-4]),int(181));
         BOOST_CHECK_EQUAL(int(buffer[test_buffer_size-5]),int(64));
    }
}

BOOST_AUTO_TEST_CASE(DeltaDeCompressor){
    const size_t test_buffer_size =1000;
    const memseries::Time t1=100;
    const memseries::Time t2=150;
    const memseries::Time t3=200;
    const memseries::Time t4=t3+100;
    const memseries::Time t5=t4+1000;
    const memseries::Time t6=t5*2;
    const memseries::Time t7=t6-50;

    uint8_t buffer[test_buffer_size];
    std::fill(std::begin(buffer),std::end(buffer),0);
    {
        Testable_DeltaCompressor co(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)));

        co.append(t1);
        co.append(t2);
        co.append(t3);
        co.append(t4);
        co.append(t5);
        co.append(t6);
        co.append(t7);
        Testable_DeltaDeCompressor dc(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)),t1);
        BOOST_CHECK_EQUAL(dc.read(),t2);
        BOOST_CHECK_EQUAL(dc.read(),t3);
        BOOST_CHECK_EQUAL(dc.read(),t4);
        BOOST_CHECK_EQUAL(dc.read(),t5);
        BOOST_CHECK_EQUAL(dc.read(),t6);
        BOOST_CHECK_EQUAL(dc.read(),t7);
    }

    std::fill(std::begin(buffer),std::end(buffer),0);
    {
        Testable_DeltaCompressor co(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)));
        memseries::Time delta=1;
        std::list<memseries::Time> times{};
        const int steps=30;
        for (int i=0;i<steps;i++){
            co.append(delta);
            times.push_back(delta);
            delta*=2;
        }

        Testable_DeltaDeCompressor dc(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)),times.front());
        times.pop_front();
        for(auto&t:times){
            auto readed=dc.read();
            BOOST_CHECK_EQUAL(readed,t);
        }
    }
	std::fill(std::begin(buffer), std::end(buffer), 0);
	{//decrease
		Testable_DeltaCompressor co(memseries::compression::BinaryBuffer(std::begin(buffer), std::end(buffer)));
		std::vector<memseries::Time> deltas{ 50,255, 1024,2050 };
		memseries::Time delta = 1000000;
		std::list<memseries::Time> times{};
		const int steps = 50;
		for (int i = 0; i<steps; i++) {
			co.append(delta);
			times.push_back(delta);
			delta -= deltas[i%deltas.size()];
		}

		Testable_DeltaDeCompressor dc(memseries::compression::BinaryBuffer(std::begin(buffer), std::end(buffer)), times.front());
		times.pop_front();
		for (auto&t : times) {
			auto readed = dc.read();
			BOOST_CHECK_EQUAL(readed, t);
		}
	}
}

BOOST_AUTO_TEST_CASE(flat_converters) {
	double pi = 3.14;
    auto ival = memseries::compression::inner::flat_double_to_int(pi);
    auto dval = memseries::compression::inner::flat_int_to_double(ival);
	BOOST_CHECK_CLOSE(dval, pi, 0.0001);
}

BOOST_AUTO_TEST_CASE(XorCompressor){
    {
        BOOST_CHECK_EQUAL(Testable_XorCompressor::zeros_lead(67553994410557440),8);
        BOOST_CHECK_EQUAL(Testable_XorCompressor::zeros_lead(3458764513820540928),2);
        BOOST_CHECK_EQUAL(Testable_XorCompressor::zeros_lead(15),60);

        BOOST_CHECK_EQUAL(Testable_XorCompressor::zeros_tail(240),4);
        BOOST_CHECK_EQUAL(Testable_XorCompressor::zeros_tail(3840),8);
    }
    const size_t test_buffer_size =1000;

    const memseries::Value t1=240;
    //const memseries::Value t2=224;

    uint8_t buffer[test_buffer_size];
    std::fill(std::begin(buffer),std::end(buffer),0);
    {
        Testable_XorCompressor dc(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)));
        BOOST_CHECK(dc.is_first());


        dc.append(t1);
        BOOST_CHECK(!dc.is_first());
        BOOST_CHECK_EQUAL(dc.get_first(),t1);
        BOOST_CHECK_EQUAL(dc.get_prev_value(),t1);
    }

    std::fill(std::begin(buffer),std::end(buffer),0);
    { // cur==prev
        Testable_XorCompressor co(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)));

        auto v1 = memseries::Value(240);
        auto v2 = memseries::Value(240);
        co.append(v1);
        co.append(v2);

        memseries::compression::XorDeCompressor dc(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)),t1);
        BOOST_CHECK_EQUAL(dc.read(),v2);
    }

    std::fill(std::begin(buffer),std::end(buffer),0);
    { // cur!=prev
        Testable_XorCompressor co(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)));

        auto v1 = memseries::Value(240);
        auto v2 = memseries::Value(96);
        auto v3 = memseries::Value(176);
        co.append(v1);
        co.append(v2);
        co.append(v3);

        memseries::compression::XorDeCompressor dc(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)),v1);
        BOOST_CHECK_EQUAL(dc.read(),v2);
        BOOST_CHECK_EQUAL(dc.read(),v3);
    }
    std::fill(std::begin(buffer),std::end(buffer),0);
    { // tail/lead is equals
        Testable_XorCompressor co(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)));

        auto v1 = memseries::Value(3840);
        auto v2 = memseries::Value(3356);
        co.append(v1);
        co.append(v2);

        memseries::compression::XorDeCompressor dc(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)),v1);
        BOOST_CHECK_EQUAL(dc.read(),v2);
    }
    std::fill(std::begin(buffer),std::end(buffer),0);
    { // tail/lead not equals
        Testable_XorCompressor co(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)));

        auto v1 = memseries::Value(3840);
        auto v2 = memseries::Value(3328);
        co.append(v1);
        co.append(v2);

        memseries::compression::XorDeCompressor dc(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)),v1);
        BOOST_CHECK_EQUAL(dc.read(),v2);
    }
    std::fill(std::begin(buffer),std::end(buffer),0);
    { 
        Testable_XorCompressor co(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)));

        std::list<memseries::Value> values{};
        memseries::Value delta=1;
        
		for(int i=0;i<100;i++){
            co.append(delta);
            values.push_back(delta);
            delta+=1;
        }

        memseries::compression::XorDeCompressor dc(memseries::compression::BinaryBuffer(std::begin(buffer),std::end(buffer)),values.front());
        values.pop_front();
        for(auto&v:values){
            BOOST_CHECK_EQUAL(dc.read(),v);
        }
    }
}

BOOST_AUTO_TEST_CASE(FlagCompressor) 
{
	const size_t test_buffer_size = 1000;
	uint8_t buffer[test_buffer_size];
	std::fill(std::begin(buffer), std::end(buffer), 0);
	memseries::compression::FlagCompressor fc(memseries::compression::BinaryBuffer(std::begin(buffer), std::end(buffer)));
	
	std::list<memseries::Flag> flags{};
	memseries::Flag delta = 1;
	for (int i = 0; i < 10; i++) {
		fc.append(delta);
		flags.push_back(delta);
		delta++;
	}

	memseries::compression::FlagDeCompressor fd(memseries::compression::BinaryBuffer(std::begin(buffer), std::end(buffer)),flags.front());
	flags.pop_front();
	for (auto f : flags) {
		auto v = fd.read();
		BOOST_CHECK_EQUAL(v, f);
	}
}


BOOST_AUTO_TEST_CASE(CompressedBlock)
{
	const size_t test_buffer_size = 1000;

	uint8_t time_begin[test_buffer_size];
	auto time_end = std::end(time_begin);

	uint8_t value_begin[test_buffer_size];
	auto value_end = std::end(value_begin);

	uint8_t flag_begin[test_buffer_size];
	auto flag_end = std::end(flag_begin);;

	std::fill(time_begin, time_end, 0);
	std::fill(flag_begin, flag_end, 0);
	std::fill(value_begin, value_end, 0);

	memseries::compression::CopmressedWriter cwr(memseries::compression::BinaryBuffer(time_begin, time_end),
		memseries::compression::BinaryBuffer(value_begin, value_end),
		memseries::compression::BinaryBuffer(flag_begin, flag_end));

	std::list<memseries::Meas> meases{};
	for (int i = 0; i < 100; i++) {
		auto m = memseries::Meas::empty();
		m.time = static_cast<memseries::Time>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count());
		m.flag = i;
		m.value = i;
		cwr.append(m);
		meases.push_back(m);
	}


    memseries::compression::CopmressedReader crr(memseries::compression::BinaryBuffer(time_begin, time_end),
		memseries::compression::BinaryBuffer(value_begin, value_end),
		memseries::compression::BinaryBuffer(flag_begin, flag_end), meases.front());

	meases.pop_front();
	for (auto &m : meases) {
		auto r_m = crr.read();
		BOOST_CHECK(m == r_m);
	}

}
