#include <libdariadb/statistic/calculator.h>
#include <gtest/gtest.h>

void check_functionkind_conversion(const dariadb::statistic::FUNCTION_KIND tested) {
  using namespace dariadb::statistic;
  std::stringstream ss;
  ss << tested;
  auto token = ss.str();
  std::istringstream oss(token);
  FUNCTION_KIND converted;
  oss >> converted;
  EXPECT_EQ(converted, tested);
}

void check_function_factory(const dariadb::statistic::FunctionKinds &tested) {
  auto result = dariadb::statistic::FunctionFactory::make(tested);
  EXPECT_EQ(result.size(), tested.size());

  auto all_functions = dariadb::statistic::FunctionFactory::functions();
  for (size_t i = 0; i < tested.size(); ++i) {
    EXPECT_EQ((int)tested[i], result[i]->kind());
    check_functionkind_conversion(tested[i]);

    EXPECT_TRUE(std::find(all_functions.begin(), all_functions.end(), tested[i]) !=
                all_functions.end())
        << "unknow function: " << tested[i];
  }
}

TEST(Statistic, Average) {
  using namespace dariadb::statistic;
  check_function_factory({FUNCTION_KIND::AVERAGE});

  auto av = dariadb::statistic::FunctionFactory::make_one(FUNCTION_KIND::AVERAGE);
  EXPECT_EQ(av->kind(), (int)dariadb::statistic::FUNCTION_KIND::AVERAGE);
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
  check_function_factory({FUNCTION_KIND::MEDIAN});

  auto median = dariadb::statistic::FunctionFactory::make_one(FUNCTION_KIND::MEDIAN);
  EXPECT_EQ(median->kind(), (int)dariadb::statistic::FUNCTION_KIND::MEDIAN);
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
  check_function_factory({FUNCTION_KIND::PERCENTILE90, FUNCTION_KIND::PERCENTILE99});

  auto p90 = dariadb::statistic::FunctionFactory::make_one(FUNCTION_KIND::PERCENTILE90);
  auto p99 = dariadb::statistic::FunctionFactory::make_one(FUNCTION_KIND::PERCENTILE99);
  EXPECT_EQ(p90->kind(), (int)dariadb::statistic::FUNCTION_KIND::PERCENTILE90);
  EXPECT_EQ(p90->result().value, dariadb::Value());

  EXPECT_EQ(p99->kind(), (int)dariadb::statistic::FUNCTION_KIND::PERCENTILE99);
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