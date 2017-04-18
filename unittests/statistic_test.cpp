#include <libdariadb/statistic/calculator.h>
#include <gtest/gtest.h>

void check_functionkind_conversion(const dariadb::statistic::FUNCKTION_KIND tested) {
  using namespace dariadb::statistic;
  std::stringstream ss;
  ss << tested;
  auto token = ss.str();
  std::istringstream oss(token);
  FUNCKTION_KIND converted;
  oss >> converted;
  EXPECT_EQ(converted, tested);
}

void check_function_factory(
    const std::vector<dariadb::statistic::FUNCKTION_KIND> &tested) {
  auto result = dariadb::statistic::FunctionFactory::make(tested);
  EXPECT_EQ(result.size(), tested.size());

  for (size_t i = 0; i < tested.size(); ++i) {
    EXPECT_EQ((int)tested[i], result[i]->kind());
    check_functionkind_conversion(tested[i]);
  }
}

TEST(Statistic, Average) {
  using namespace dariadb::statistic;
  check_function_factory({FUNCKTION_KIND::AVERAGE});

  dariadb::statistic::Average av;
  EXPECT_EQ(av.kind(), (int)dariadb::statistic::FUNCKTION_KIND::AVERAGE);
  EXPECT_EQ(av.result().value, dariadb::Value());
  dariadb::Meas m;

  m.value = 2;
  av.apply(m);
  EXPECT_EQ(av.result().value, dariadb::Value(2));

  m.value = 6;
  av.apply(m);
  EXPECT_EQ(av.result().value, dariadb::Value(4));
}
