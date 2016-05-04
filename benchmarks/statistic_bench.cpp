#include <cstdlib>
#include <ctime>
#include <iostream>
#include <iterator>

#include <chrono>
#include <cmath>
#include <ctime>
#include <limits>
#include <math/statistic.h>

const size_t K = 5;

float bench_method(dariadb::statistic::BaseMethod *bi) {
  auto start = clock();
  auto m = dariadb::Meas::empty();
  for (size_t i = 1; i < K * 1000000; i++) {
    m.value = dariadb::Value(i);
    bi->call(m);
  }
  auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
  return elapsed / K;
}

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;
  using dariadb::statistic::integral::RectangleMethod;
  {
    std::unique_ptr<RectangleMethod> p{
        new RectangleMethod(RectangleMethod::Kind::LEFT)};

    auto elapsed = bench_method(p.get());
    std::cout << "rectangle left: " << elapsed << std::endl;
  }
  {
    std::unique_ptr<RectangleMethod> p{
        new RectangleMethod(RectangleMethod::Kind::RIGHT)};

    auto elapsed = bench_method(p.get());
    std::cout << "rectangle right: " << elapsed << std::endl;
  }
  {
    std::unique_ptr<RectangleMethod> p{
        new RectangleMethod(RectangleMethod::Kind::MIDLE)};

    auto elapsed = bench_method(p.get());
    std::cout << "rectangle midle: " << elapsed << std::endl;
  }
  using dariadb::statistic::average::Average;
  {
    std::unique_ptr<Average> p{new Average()};

    auto elapsed = bench_method(p.get());
    std::cout << "average: " << elapsed << std::endl;
  }
}
