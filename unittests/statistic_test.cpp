#include <libdariadb/statistic/calculator.h>
#include <gtest/gtest.h>

TEST(Statistic, Average) {
  dariadb::statistic::Average av;
  EXPECT_EQ(av.result().value, dariadb::Value());
}
