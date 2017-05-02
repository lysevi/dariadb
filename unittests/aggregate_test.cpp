#include <libdariadb/aggregate/aggregator.h>
#include <libdariadb/aggregate/timer.h>
#include <libdariadb/dariadb.h>
#include <libdariadb/timeutil.h>
#include <gtest/gtest.h>

class Aggregate : public testing::Test {
protected:
  virtual void SetUp() {
    _storage = dariadb::memory_only_storage();
    _scheme = dariadb::scheme::Scheme::create(_storage->settings());
  }

  virtual void TearDown() {
    _scheme = nullptr;
    _storage = nullptr;
  }

  dariadb::IEngine_Ptr _storage;
  dariadb::scheme::IScheme_Ptr _scheme;
};

TEST_F(Aggregate, EmptyScheme) {
  using namespace dariadb::aggregator;
  using namespace dariadb;
  Aggreagator::aggregate("raw", "minute", IEngine_Ptr(), MIN_TIME, MAX_TIME);
  Aggreagator::aggregate("raw", "minute", _storage, MIN_TIME, MAX_TIME);
}

TEST_F(Aggregate, DownsamplingIntervals) {
  using namespace dariadb::aggregator;
  using namespace dariadb::timeutil;
  using namespace dariadb::storage;

  _storage->setScheme(_scheme);
  auto raw_id = _scheme->addParam("param1.raw");
  auto minute_id = _scheme->addParam("param1.average.minute");
  auto half_id = _scheme->addParam("param1.average.halfhour");
  auto hour_id = _scheme->addParam("param1.average.hour");
  auto day_id = _scheme->addParam("param1.average.day");
  auto week_id = _scheme->addParam("param1.average.week");
  auto month_id = _scheme->addParam("param1.average.month31");

  DateTime dt;
  dt.year = 2017;
  dt.month = 1;
  dt.day = 1;
  dt.hour = 0;
  dt.minute = 0;
  dt.second = 0;
  dt.millisecond = 0;

  _storage->append(raw_id, from_datetime(dt), 2);
  dt.second = 59;
  _storage->append(raw_id, from_datetime(dt), 4);
  { // raw -> minute
    auto interval = target_interval("minute", from_datetime(dt));
    Aggreagator::aggregate("raw", "minute", _storage, interval.first, interval.second);

    dariadb::QueryInterval qi({minute_id, half_id}, 0, dariadb::MIN_TIME,
                              dariadb::MAX_TIME);
    auto values = _storage->readInterval(qi);

    EXPECT_EQ(values.size(), size_t(1));
    EXPECT_EQ(values.front().id, minute_id);
    EXPECT_EQ(values.front().value, dariadb::Value(3));
  }
  { // minute->halfhour
    auto interval = target_interval("halfhour", from_datetime(dt));
    Aggreagator::aggregate("minute", "halfhour", _storage, interval.first,
                           interval.second);

    dariadb::QueryInterval qi({half_id, hour_id}, 0, dariadb::MIN_TIME,
                              dariadb::MAX_TIME);
    auto values = _storage->readInterval(qi);

    EXPECT_EQ(values.size(), size_t(1));
    EXPECT_EQ(values.front().id, half_id);
    EXPECT_EQ(values.front().value, dariadb::Value(3));
  }

  { // halfhour->hour
    auto interval = target_interval("hour", from_datetime(dt));
    Aggreagator::aggregate("halfhour", "hour", _storage, interval.first, interval.second);

    dariadb::QueryInterval qi({hour_id, day_id}, 0, dariadb::MIN_TIME, dariadb::MAX_TIME);
    auto values = _storage->readInterval(qi);

    EXPECT_EQ(values.size(), size_t(1));
    EXPECT_EQ(values.front().id, hour_id);
    EXPECT_EQ(values.front().value, dariadb::Value(3));
  }

  { // hour->day
    auto interval = target_interval("hour", from_datetime(dt));
    Aggreagator::aggregate("hour", "day", _storage, interval.first, interval.second);

    dariadb::QueryInterval qi({day_id, week_id}, 0, dariadb::MIN_TIME, dariadb::MAX_TIME);
    auto values = _storage->readInterval(qi);

    EXPECT_EQ(values.size(), size_t(1));
    EXPECT_EQ(values.front().id, day_id);
    EXPECT_EQ(values.front().value, dariadb::Value(3));
  }

  { // day->week
    auto interval = target_interval("week", from_datetime(dt));
    Aggreagator::aggregate("day", "week", _storage, interval.first, interval.second);

    dariadb::QueryInterval qi({week_id, month_id}, 0, dariadb::MIN_TIME,
                              dariadb::MAX_TIME);
    auto values = _storage->readInterval(qi);

    EXPECT_EQ(values.size(), size_t(1));
    EXPECT_EQ(values.front().id, week_id);
    EXPECT_EQ(values.front().value, dariadb::Value(3));
  }

  { // week->month
    auto interval = target_interval("month31", from_datetime(dt));
    Aggreagator::aggregate("week", "month31", _storage, interval.first, interval.second);

    dariadb::QueryInterval qi({month_id}, 0, dariadb::MIN_TIME, dariadb::MAX_TIME);
    auto values = _storage->readInterval(qi);

    EXPECT_EQ(values.size(), size_t(1));
    EXPECT_EQ(values.front().id, month_id);
    EXPECT_EQ(values.front().value, dariadb::Value(3));
  }
}