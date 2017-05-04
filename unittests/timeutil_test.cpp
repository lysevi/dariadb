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
    auto hh = dariadb::timeutil::intervalName2time("month");
    auto dt = dariadb::timeutil::to_datetime(hh);
    EXPECT_EQ(dt.minute, uint8_t(0));
    EXPECT_EQ(dt.hour, uint8_t(0));
    EXPECT_EQ(dt.day, uint8_t(1));
    EXPECT_EQ(dt.month, uint8_t(2));
  }
  {
	  auto hh = dariadb::timeutil::intervalName2time("year");
	  auto dt = dariadb::timeutil::to_datetime(hh);
	  EXPECT_EQ(dt.year, uint16_t(1971));
	  EXPECT_EQ(dt.minute, uint8_t(0));
	  EXPECT_EQ(dt.hour, uint8_t(0));
	  EXPECT_EQ(dt.day, uint8_t(1));
	  EXPECT_EQ(dt.month, uint8_t(1));
  }
}

TEST(Time, IntervalCompare) {
  using namespace dariadb::timeutil;
  EXPECT_TRUE(intervalsLessCmp("minute", "halfhour"));
  EXPECT_TRUE(intervalsLessCmp("halfhour", "hour"));
  EXPECT_FALSE(intervalsLessCmp("halfhour", "halfhour"));
  EXPECT_TRUE(intervalsLessCmp("hour", "day"));
  EXPECT_TRUE(intervalsLessCmp("day", "week"));
  EXPECT_TRUE(intervalsLessCmp("week", "month"));
  EXPECT_TRUE(intervalsLessCmp("month", "year"));
}

TEST(Time, IntervalPredefined) {
  using namespace dariadb::timeutil;
  EXPECT_EQ(predefinedIntervals().size(), size_t(7));
}

TEST(Time, DateTime) {
  using namespace dariadb::timeutil;
  DateTime dt1;
  dt1.year = 2017;
  dt1.month = 3;
  dt1.day = 5;
  dt1.hour = 11;
  dt1.minute = 45;
  dt1.second = 55;
  dt1.millisecond = 11;

  auto t = from_datetime(dt1);

  auto dt2 = to_datetime(t);
  EXPECT_EQ(dt1.year, dt2.year);
  EXPECT_EQ(dt1.month, dt2.month);
  EXPECT_EQ(dt1.day, dt2.day);
  EXPECT_EQ(dt1.hour, dt2.hour);
  EXPECT_EQ(dt1.minute, dt2.minute);
  EXPECT_EQ(dt1.second, dt2.second);
  EXPECT_EQ(dt1.millisecond, dt2.millisecond);
}

TEST(Time, TargetInterval) {
  using namespace dariadb::timeutil;

  DateTime dt;
  dt.year = 2017;
  dt.month = 1;
  dt.day = 1;
  dt.hour = 23;
  dt.minute = 59;
  dt.second = 1;
  dt.millisecond = 1;
  // all result must be 1.02.17 (except halfhour2)
  auto time_dt = from_datetime(dt);
  {
    auto to_minute = target_interval("minute", time_dt);

    auto result_dt = to_datetime(to_minute.second);
    EXPECT_EQ(result_dt.day, dt.day);
    EXPECT_EQ(result_dt.hour, dt.hour);
    EXPECT_EQ(result_dt.minute, dt.minute);
    EXPECT_EQ(result_dt.second, 59);
    EXPECT_EQ(result_dt.millisecond, 999);

    auto end_dt = to_datetime(to_minute.first);
    EXPECT_EQ(end_dt.day, dt.day);
    EXPECT_EQ(end_dt.hour, dt.hour);
    EXPECT_EQ(end_dt.minute, dt.minute);
    EXPECT_EQ(end_dt.second, 0);
    EXPECT_EQ(end_dt.millisecond, 0);
  }
  {
    auto to_hh = target_interval("halfhour", time_dt);

    auto result_dt = to_datetime(to_hh.second);
    EXPECT_EQ(result_dt.day, dt.day);
    EXPECT_EQ(result_dt.hour, dt.hour);
    EXPECT_EQ(result_dt.minute, dt.minute);
    EXPECT_EQ(result_dt.second, 59);
    EXPECT_EQ(result_dt.millisecond, 999);

    result_dt = to_datetime(to_hh.first);
    EXPECT_EQ(result_dt.day, dt.day);
    EXPECT_EQ(result_dt.hour, dt.hour);
    EXPECT_EQ(result_dt.minute, 30);
    EXPECT_EQ(result_dt.second, 0);
    EXPECT_EQ(result_dt.millisecond, 0);
  }

  {
    DateTime dt_begin = dt;
    dt_begin.minute = 3;
    auto time_dt_begin = from_datetime(dt_begin);
    auto to_hh = target_interval("halfhour", time_dt_begin);

    auto result_dt = to_datetime(to_hh.second);
    EXPECT_EQ(result_dt.day, dt.day);
    EXPECT_EQ(result_dt.hour, dt.hour);
    EXPECT_EQ(result_dt.minute, 29);
    EXPECT_EQ(result_dt.second, 59);
    EXPECT_EQ(result_dt.millisecond, 999);

    result_dt = to_datetime(to_hh.first);
    EXPECT_EQ(result_dt.day, dt.day);
    EXPECT_EQ(result_dt.hour, dt.hour);
    EXPECT_EQ(result_dt.minute, 0);
    EXPECT_EQ(result_dt.second, 0);
    EXPECT_EQ(result_dt.millisecond, 0);
  }

  {
    auto to_hr = target_interval("hour", time_dt);

    auto result_dt = to_datetime(to_hr.second);
    EXPECT_EQ(result_dt.day, dt.day);
    EXPECT_EQ(result_dt.hour, dt.hour);
    EXPECT_EQ(result_dt.minute, dt.minute);
    EXPECT_EQ(result_dt.second, 59);
    EXPECT_EQ(result_dt.millisecond, 999);

    result_dt = to_datetime(to_hr.first);
    EXPECT_EQ(result_dt.day, dt.day);
    EXPECT_EQ(result_dt.hour, dt.hour);
    EXPECT_EQ(result_dt.minute, 0);
    EXPECT_EQ(result_dt.second, 0);
    EXPECT_EQ(result_dt.millisecond, 0);
  }

  {
    auto to_hr = target_interval("day", time_dt);

    auto result_dt = to_datetime(to_hr.second);
    EXPECT_EQ(result_dt.day, dt.day);
    EXPECT_EQ(result_dt.hour, 23);
    EXPECT_EQ(result_dt.minute, 59);
    EXPECT_EQ(result_dt.second, 59);
    EXPECT_EQ(result_dt.millisecond, 999);

    result_dt = to_datetime(to_hr.first);
    EXPECT_EQ(result_dt.day, dt.day);
    EXPECT_EQ(result_dt.hour, 0);
    EXPECT_EQ(result_dt.minute, 0);
    EXPECT_EQ(result_dt.second, 0);
    EXPECT_EQ(result_dt.millisecond, 0);
  }

  {
    auto to_hr = target_interval("week", time_dt);

    auto result_dt = to_datetime(to_hr.second);
    EXPECT_EQ(result_dt.day, dt.day);
    EXPECT_EQ(result_dt.hour, 23);
    EXPECT_EQ(result_dt.minute, 59);
    EXPECT_EQ(result_dt.second, 59);
    EXPECT_EQ(result_dt.millisecond, 999);

    result_dt = to_datetime(to_hr.first);
    EXPECT_EQ(result_dt.day, 26);
    EXPECT_EQ(result_dt.hour, 0);
    EXPECT_EQ(result_dt.minute, 0);
    EXPECT_EQ(result_dt.second, 0);
    EXPECT_EQ(result_dt.millisecond, 0);
  }

  {
    auto to_hr = target_interval("month", time_dt);

    auto result_dt = to_datetime(to_hr.second);
    EXPECT_EQ(result_dt.month, dt.month);
    EXPECT_EQ(result_dt.day, 31);
    EXPECT_EQ(result_dt.hour, 23);
    EXPECT_EQ(result_dt.minute, 59);
    EXPECT_EQ(result_dt.second, 59);
    EXPECT_EQ(result_dt.millisecond, 999);

    result_dt = to_datetime(to_hr.first);
    EXPECT_EQ(result_dt.month, dt.month);
    EXPECT_EQ(result_dt.day, 1);
    EXPECT_EQ(result_dt.hour, 0);
    EXPECT_EQ(result_dt.minute, 0);
    EXPECT_EQ(result_dt.second, 0);
    EXPECT_EQ(result_dt.millisecond, 0);
  }


  {
	  auto to_hr = target_interval("year", time_dt);

	  auto result_dt = to_datetime(to_hr.second);
	  EXPECT_EQ(result_dt.year, dt.year);
	  EXPECT_EQ(result_dt.month, 12);
	  EXPECT_EQ(result_dt.day, 31);
	  EXPECT_EQ(result_dt.hour, 23);
	  EXPECT_EQ(result_dt.minute, 59);
	  EXPECT_EQ(result_dt.second, 59);
	  EXPECT_EQ(result_dt.millisecond, 999);

	  result_dt = to_datetime(to_hr.first);
	  EXPECT_EQ(result_dt.month, 1);
	  EXPECT_EQ(result_dt.day, 1);
	  EXPECT_EQ(result_dt.hour, 0);
	  EXPECT_EQ(result_dt.minute, 0);
	  EXPECT_EQ(result_dt.second, 0);
	  EXPECT_EQ(result_dt.millisecond, 0);
  }
}