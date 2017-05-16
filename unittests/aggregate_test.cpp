
#include <libdariadb/aggregate/aggregator.h>
#include <libdariadb/aggregate/timer.h>
#include <libdariadb/dariadb.h>
#include <libdariadb/timeutil.h>
#include "helpers.h"
#include <catch.hpp>

class Aggregate {
public:
  Aggregate() {
    _storage = dariadb::memory_only_storage();
    _scheme = dariadb::scheme::Scheme::create(_storage->settings());
  }

  ~Aggregate() {
    _scheme = nullptr;
    _storage = nullptr;
  }

  dariadb::IEngine_Ptr _storage;
  dariadb::scheme::IScheme_Ptr _scheme;
};

class MockTimer : public dariadb::aggregator::ITimer {
public:
  MockTimer(dariadb::Time curTime) : _ctime(curTime) {}
  void addCallback(dariadb::Time firstTime,
                   dariadb::aggregator::ITimer::Callback_Ptr clbk) override {
    _callbacks.push_back(std::make_pair(firstTime, clbk));
  }

  dariadb::Time currentTime() const override { return _ctime; }

  std::list<std::pair<dariadb::Time, dariadb::aggregator::ITimer::Callback_Ptr>>
      _callbacks;

  dariadb::Time _ctime;
};

TEST_CASE_METHOD(Aggregate, "EmptyScheme") {
  using namespace dariadb::aggregator;
  using namespace dariadb;
  Aggregator::aggregate("raw", "minute", IEngine_Ptr(), MIN_TIME, MAX_TIME);
  Aggregator::aggregate("raw", "minute", _storage, MIN_TIME, MAX_TIME);
}

TEST_CASE_METHOD(Aggregate, "DownsamplingIntervals") {
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
  auto month_id = _scheme->addParam("param1.average.month");

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
    Aggregator::aggregate("raw", "minute", _storage, interval.first, interval.second);

    dariadb::QueryInterval qi({minute_id, half_id}, 0, dariadb::MIN_TIME,
                              dariadb::MAX_TIME);
    auto values = _storage->readInterval(qi);

    EXPECT_EQ(values.size(), size_t(1));
    EXPECT_EQ(values.front().id, minute_id);
    EXPECT_EQ(values.front().value, dariadb::Value(3));
  }
  { // minute->halfhour
    auto interval = target_interval("halfhour", from_datetime(dt));
    Aggregator::aggregate("minute", "halfhour", _storage, interval.first,
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
    Aggregator::aggregate("halfhour", "hour", _storage, interval.first, interval.second);

    dariadb::QueryInterval qi({hour_id, day_id}, 0, dariadb::MIN_TIME, dariadb::MAX_TIME);
    auto values = _storage->readInterval(qi);

    EXPECT_EQ(values.size(), size_t(1));
    EXPECT_EQ(values.front().id, hour_id);
    EXPECT_EQ(values.front().value, dariadb::Value(3));
  }

  { // hour->day
    auto interval = target_interval("hour", from_datetime(dt));
    Aggregator::aggregate("hour", "day", _storage, interval.first, interval.second);

    dariadb::QueryInterval qi({day_id, week_id}, 0, dariadb::MIN_TIME, dariadb::MAX_TIME);
    auto values = _storage->readInterval(qi);

    EXPECT_EQ(values.size(), size_t(1));
    EXPECT_EQ(values.front().id, day_id);
    EXPECT_EQ(values.front().value, dariadb::Value(3));
  }

  { // day->week
    auto interval = target_interval("week", from_datetime(dt));
    Aggregator::aggregate("day", "week", _storage, interval.first, interval.second);

    dariadb::QueryInterval qi({week_id, month_id}, 0, dariadb::MIN_TIME,
                              dariadb::MAX_TIME);
    auto values = _storage->readInterval(qi);

    EXPECT_EQ(values.size(), size_t(1));
    EXPECT_EQ(values.front().id, week_id);
    EXPECT_EQ(values.front().value, dariadb::Value(3));
  }

  { // week->month
    auto interval = target_interval("month", from_datetime(dt));
    Aggregator::aggregate("week", "month", _storage, interval.first, interval.second);

    dariadb::QueryInterval qi({month_id}, 0, dariadb::MIN_TIME, dariadb::MAX_TIME);
    auto values = _storage->readInterval(qi);

    EXPECT_EQ(values.size(), size_t(1));
    EXPECT_EQ(values.front().id, month_id);
    EXPECT_EQ(values.front().value, dariadb::Value(3));
  }
}

TEST_CASE_METHOD(Aggregate, "Aggregator") {
  using namespace dariadb::aggregator;
  using namespace dariadb::timeutil;
  
  _storage->setScheme(_scheme);

  DateTime dt;
  dt.year = 2017;
  dt.month = 1;
  dt.day = 1;
  dt.hour = 0;
  dt.minute = 0;
  dt.second = 0;
  dt.millisecond = 0;

  auto raw_timer = new MockTimer(from_datetime(dt));
  ITimer_Ptr mock_timer(raw_timer);
  
  Aggregator agg(_storage, mock_timer);

  // raw=>minute, minute=>halfhour, halfhour=>hour, hour=>day, day=>week, week=>month;
  EXPECT_EQ(raw_timer->_callbacks.size(), predefinedIntervals().size());

  dt.second = 59;
  dt.millisecond = 999;
  auto first_minute = from_datetime(dt);
  raw_timer->_ctime = first_minute;

  bool exists = false;
  for (auto c : raw_timer->_callbacks) {
    if (c.first == first_minute) {
      exists = true;
      auto next_minute = c.second->apply(first_minute);
      dt = to_datetime(next_minute);
      EXPECT_EQ(dt.minute, 1);
      EXPECT_EQ(dt.second, 59);
      EXPECT_EQ(dt.millisecond, 999);
      break;
    }
  }
  EXPECT_TRUE(exists);
}

class MockTimerCallback : public dariadb::aggregator::ITimer::Callback {
public:
  MockTimerCallback(size_t *calls) { calls_ptr = calls; }
  virtual dariadb::Time apply(dariadb::Time current_time) override {
    *calls_ptr = (*calls_ptr) + 1;
    return current_time + 15 * (*calls_ptr);
  }
  size_t *calls_ptr;
};

TEST_CASE_METHOD(Aggregate, "Timer") {
  using namespace dariadb::aggregator;
  using namespace dariadb::timeutil;

  Timer tm;
  size_t calls = 0;
  ITimer::Callback_Ptr clbk(new MockTimerCallback(&calls));
  tm.addCallback(current_time() - 10, clbk);

  while (calls < 5) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    dariadb::logger_fatal("calls: ", calls);
  }
}