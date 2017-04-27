#include <libdariadb/timeutil.h>

#include <gtest/gtest.h>

TEST(Time, TimeToString) {
  auto ct = dariadb::timeutil::current_time();
  EXPECT_TRUE(ct != dariadb::Time(0));
  auto ct_str = dariadb::timeutil::to_string(ct);
  EXPECT_TRUE(ct_str.size() != 0);
}

TEST(Time, TimeRound) {
  auto ct = dariadb::timeutil::current_time();
  {
    auto rounded = dariadb::timeutil::round_to_seconds(ct);
    auto rounded_d = dariadb::timeutil::to_datetime(rounded);
    EXPECT_EQ(rounded_d.millisecond, uint16_t(0));
  }

  {
    auto rounded = dariadb::timeutil::round_to_minutes(ct);
    auto rounded_d = dariadb::timeutil::to_datetime(rounded);
    EXPECT_EQ(rounded_d.millisecond, uint16_t(0));
    EXPECT_EQ(rounded_d.second, uint16_t(0));
  }

  {
    auto rounded = dariadb::timeutil::round_to_hours(ct);
    auto rounded_d = dariadb::timeutil::to_datetime(rounded);
    EXPECT_EQ(rounded_d.millisecond, uint16_t(0));
    EXPECT_EQ(rounded_d.second, uint16_t(0));
    EXPECT_EQ(rounded_d.minute, uint16_t(0));
  }
}

TEST(Time, IntervalFromString) {
  {
    auto hh = dariadb::timeutil::intervalName2time("minute");
    auto dt = dariadb::timeutil::to_datetime(hh);
    EXPECT_EQ(dt.minute, uint8_t(1));
  }
  {
    auto hh = dariadb::timeutil::intervalName2time("halfhour");
    auto dt = dariadb::timeutil::to_datetime(hh);
    EXPECT_EQ(dt.minute, uint8_t(30));
  }
  {
    auto hh = dariadb::timeutil::intervalName2time("hour");
    auto dt = dariadb::timeutil::to_datetime(hh);
    EXPECT_EQ(dt.minute, uint8_t(0));
    EXPECT_EQ(dt.hour, uint8_t(1));
  }
  {
    auto hh = dariadb::timeutil::intervalName2time("day");
    auto dt = dariadb::timeutil::to_datetime(hh);
    EXPECT_EQ(dt.minute, uint8_t(0));
    EXPECT_EQ(dt.hour, uint8_t(0));
    EXPECT_EQ(dt.day, uint8_t(2)); // boost datetime count of full days.
  }
  {
    auto hh = dariadb::timeutil::intervalName2time("week");
    auto dt = dariadb::timeutil::to_datetime(hh);
    EXPECT_EQ(dt.minute, uint8_t(0));
    EXPECT_EQ(dt.hour, uint8_t(0));
    EXPECT_EQ(dt.day, uint8_t(8));
  }
  {
    auto hh = dariadb::timeutil::intervalName2time("month31");
    auto dt = dariadb::timeutil::to_datetime(hh);
    EXPECT_EQ(dt.minute, uint8_t(0));
    EXPECT_EQ(dt.hour, uint8_t(0));
    EXPECT_EQ(dt.day, uint8_t(1));
    EXPECT_EQ(dt.month, uint8_t(2));
  }
}

TEST(Time, IntervalCompare) {
  using namespace dariadb::timeutil;
  EXPECT_TRUE(intervalsLessCmp("minute", "halfhour"));
  EXPECT_TRUE(intervalsLessCmp("halfhour", "hour"));
  EXPECT_FALSE(intervalsLessCmp("halfhour", "halfhour"));
  EXPECT_TRUE(intervalsLessCmp("hour", "day"));
  EXPECT_TRUE(intervalsLessCmp("day", "week"));
  EXPECT_TRUE(intervalsLessCmp("week", "month31"));
}

TEST(Time, IntervalPredefined) {
  using namespace dariadb::timeutil;
  EXPECT_EQ(predefinedIntervals().size(), size_t(6));
}