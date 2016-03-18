#include <ctime>
#include <iostream>
#include <cstdlib>
#include <iterator>

#include <integral.h>
#include <ctime>
#include <limits>
#include <cmath>
#include <chrono>

const size_t K = 5;

float bench_int(dariadb::statistic::BaseIntegral*bi){
	auto start = clock();
    auto m=dariadb::Meas::empty();
    for (size_t i = 1; i < K*1000000; i++) {
        m.value=i;
        bi->call(m);
    }
	auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
	return elapsed/K;
}

int main(int argc, char *argv[]) {
	(void)argc;
	(void)argv;
	using dariadb::statistic::RectangleMethod;
	{
		std::unique_ptr<RectangleMethod>  p{ new RectangleMethod(RectangleMethod::Kind::LEFT) };

		auto elapsed = bench_int(p.get());
		std::cout << "rectangle left: " << elapsed << std::endl;
	}
	{
		std::unique_ptr<RectangleMethod>  p{ new RectangleMethod(RectangleMethod::Kind::RIGHT) };

		auto elapsed = bench_int(p.get());
		std::cout << "rectangle right: " << elapsed << std::endl;
	}
	{
		std::unique_ptr<RectangleMethod>  p{ new RectangleMethod(RectangleMethod::Kind::MIDLE) };

		auto elapsed = bench_int(p.get());
		std::cout << "rectangle midle: " << elapsed << std::endl;
	}
}
