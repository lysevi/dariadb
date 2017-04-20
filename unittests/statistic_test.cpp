#include <libdariadb/dariadb.h>
#include <libdariadb/statistic/calculator.h>
#include <gtest/gtest.h>

void check_function_factory(const std::vector<std::string> &tested) {
  auto result = dariadb::statistic::FunctionFactory::make(tested);
  EXPECT_EQ(result.size(), tested.size());

  auto all_functions = dariadb::statistic::FunctionFactory::functions();
  for (size_t i = 0; i < tested.size(); ++i) {
    EXPECT_NE(result[i], nullptr);
    EXPECT_EQ(tested[i], result[i]->kind());

    EXPECT_TRUE(std::find(all_functions.begin(), all_functions.end(), tested[i]) !=
                all_functions.end())
        << "unknow function: " << tested[i];
  }
}

TEST(Statistic, Average) {
  using namespace dariadb::statistic;
  check_function_factory({"average"});

  auto av = dariadb::statistic::FunctionFactory::make_one("AVerage");
  EXPECT_EQ(av->kind(), "average");
  EXPECT_EQ(av->result().value, dariadb::Value());

  dariadb::Meas m;

  m.value = 2;
  av->apply(m);
  EXPECT_EQ(av->result().value, dariadb::Value(2));

  m.value = 6;
  av->apply(m);
  EXPECT_EQ(av->result().value, dariadb::Value(4));
}

TEST(Statistic, Median) {
  using namespace dariadb::statistic;
  check_function_factory({"median"});

  auto median = dariadb::statistic::FunctionFactory::make_one("Median");
  EXPECT_EQ(median->kind(), "median");
  EXPECT_EQ(median->result().value, dariadb::Value());
  dariadb::Meas m;

  m.value = 2;
  median->apply(m);
  EXPECT_EQ(median->result().value, dariadb::Value(2));

  m.value = 6;
  median->apply(m);
  EXPECT_EQ(median->result().value, dariadb::Value(4));

  m.value = 3;
  median->apply(m);
  EXPECT_EQ(median->result().value, dariadb::Value(3));

  m.value = 0;
  median->apply(m);
  EXPECT_EQ(median->result().value, dariadb::Value(3));
}

TEST(Statistic, Percentile) {
  using namespace dariadb::statistic;
  check_function_factory({"percentile90", "percentile99"});

  auto p90 = dariadb::statistic::FunctionFactory::make_one("Percentile90");
  auto p99 = dariadb::statistic::FunctionFactory::make_one("Percentile99");
  EXPECT_EQ(p90->kind(), "percentile90");
  EXPECT_EQ(p90->result().value, dariadb::Value());

  EXPECT_EQ(p99->kind(), "percentile99");
  EXPECT_EQ(p99->result().value, dariadb::Value());

  dariadb::Meas m;
  std::vector<dariadb::Value> values{43, 54, 56, 61, 62, 66, 68, 69, 69, 70, 71, 72, 77,
                                     78, 79, 85, 87, 88, 89, 93, 95, 96, 98, 99, 99};
  for (auto v : values) {
    m.value = v;
    p90->apply(m);
    p99->apply(m);
  }
  EXPECT_EQ(p90->result().value, dariadb::Value(98));
  EXPECT_EQ(p99->result().value, dariadb::Value(99));
}

TEST(Statistic, Sigma) {
  using namespace dariadb::statistic;
  check_function_factory({"sigma"});

  auto sigma = dariadb::statistic::FunctionFactory::make_one("Sigma");
  EXPECT_EQ(sigma->kind(), "sigma");
  EXPECT_EQ(sigma->result().value, dariadb::Value());
  dariadb::Meas m;

  sigma->apply(m);
  sigma->apply(m);

  m.value = 14.0;
  sigma->apply(m);
  sigma->apply(m);

  auto calulated = sigma->result().value;
  EXPECT_DOUBLE_EQ(calulated, 7.0);
}

TEST(Statistic, Calculator) {
  auto storage = dariadb::open_storage("");
  dariadb::Meas meas;
  meas.id = 10;
  for (size_t i = 0; i < 100; ++i) {
    meas.value++;
    meas.time++;
    storage->append(meas);
  }
  dariadb::statistic::Calculator calc(storage);
  auto all_functions = dariadb::statistic::FunctionFactory::functions();
  all_functions.push_back("UNKNOW FUNCTION");
  auto result = calc.apply(dariadb::Id(10), dariadb::Time(0), meas.time, dariadb::Flag(),
                           all_functions);

  EXPECT_EQ(result.size(), all_functions.size());
  for (size_t i = 0; i < all_functions.size() - 1; ++i) {
    auto m = result[i];
    EXPECT_EQ(m.id, dariadb::Id(10));
    EXPECT_TRUE(m.value != dariadb::Value());
  }

  EXPECT_EQ(result.back().value, 0);
}