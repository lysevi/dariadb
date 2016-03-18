#include <ctime>
#include <iostream>
#include <cstdlib>
#include <iterator>

#include <integral.h>
#include <ctime>
#include <limits>
#include <cmath>
#include <chrono>

const size_t K = 1;


class Moc_I1:public memseries::statistic::BaseIntegral{
public:
    Moc_I1(){
        _a=_b=memseries::Meas::empty();
    }
    void calc(const memseries::Meas&a,const memseries::Meas&b)override{
        _a=a;
        _b=b;
    }
	memseries::Value result()const {
		return memseries::Value();
	}
    memseries::Meas _a;
    memseries::Meas _b;
};


float bench_int(memseries::statistic::BaseIntegral*bi){
	auto start = clock();
    auto m=memseries::Meas::empty();
    for (size_t i = 1; i < K*1000000; i++) {
        m.value=i;
        bi->call(m);
    }
	auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
	return elapsed;
}

int main(int argc, char *argv[]) {
	(void)argc;
	(void)argv;
    std::unique_ptr<Moc_I1>  p{new Moc_I1};
  
    auto elapsed= bench_int(p.get());
    std::cout << "integrall calls: " << elapsed << std::endl;
}
