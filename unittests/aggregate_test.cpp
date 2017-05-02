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

TEST_F(Aggregate, Minute) {
  using namespace dariadb::aggregator;
  using namespace dariadb::timeutil;
  using namespace dariadb::storage;

  _storage->setScheme(_scheme);
  auto raw_id = _scheme->addParam("param1.raw");
  auto minute_id = _scheme->addParam("param1.average.minute");
  auto hour_id = _scheme->addParam("param1.average.hour");

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
  auto interval = target_interval("minute", from_datetime(dt));
  Aggreagator::aggregate("raw", "minute", _storage, interval.first, interval.second);

  dariadb::QueryInterval qi({minute_id, hour_id}, 0, dariadb::MIN_TIME,
                            dariadb::MAX_TIME);
  auto values = _storage->readInterval(qi);

  EXPECT_EQ(values.size(), size_t(1));
  EXPECT_EQ(values.front().id, minute_id);
  EXPECT_EQ(values.front().value, dariadb::Value(3));
}
