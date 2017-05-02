#include <libdariadb/aggregate/aggregator.h>
#include <libdariadb/aggregate/timer.h>
#include <libdariadb/scheme/ischeme.h>
#include <libdariadb/statistic/calculator.h>
#include <libdariadb/timeutil.h>
#include <sstream>

using namespace dariadb;
using namespace dariadb::aggregator;
using namespace dariadb::storage;
using namespace dariadb::timeutil;
using namespace dariadb::scheme;

namespace {
class TimerCallback : public ITimer::Callback {
public:
  TimerCallback(const std::string &interval_from, const std::string &interval_to,
                IEngine_Ptr storage) {
    _ifrom = interval_from;
    _ito = interval_to;
    _storage = storage;
  }

  Time apply(Time current_time) override {
    logger_info("aggregator: callback for '", _ito, "': ", to_string(current_time));

    auto delta = timeutil::intervalName2time(_ito);
    auto interval_to_target = timeutil::target_interval(_ito, current_time);
    Aggregator::aggregate(_ifrom, _ito, _storage, interval_to_target.first,
                          interval_to_target.second);

    // move to half of next interval.
    current_time += delta / 2;
    interval_to_target = timeutil::target_interval(_ito, current_time);
    logger_info("aggregator: next call for '", _ito,
                "': ", to_string(interval_to_target.second));
    return interval_to_target.second;
  }

  std::string _ifrom, _ito;
  IEngine_Ptr _storage;
};
} // namespace

class Aggregator::Private {
public:
  Private(IEngine_Ptr storage) : _timer(new Timer), _storage(storage) { fill_timer(); }

  Private(IEngine_Ptr storage, ITimer_Ptr timer) : _timer(timer), _storage(storage) {
    fill_timer();
  }
  ~Private() { _timer = nullptr; }

  void fill_timer() {
    if (_scheme == nullptr) {
      _scheme = _storage->getScheme();
    }
    if (_scheme == nullptr) {
      logger_fatal("aggregator: can't fill timer. scheme does not set.");
      return;
    }

    logger_info("agregator: init timers.");
    auto all_intervals = predefinedIntervals();
    all_intervals.insert(all_intervals.begin(), "raw");
    ENSURE(all_intervals.front() == "raw");

    for (size_t i = 0; i < all_intervals.size() - 1; ++i) {
      auto interval_from = all_intervals[i];
      auto interval_to = all_intervals[i];

      ITimer::Callback_Ptr clbk{new TimerCallback(interval_from, interval_to, _storage)};
      auto interval_to_target =
          timeutil::target_interval(interval_to, _timer->currentTime());
      _timer->addCallback(interval_to_target.second, clbk);
    }
  }
  ITimer_Ptr _timer;
  IEngine_Ptr _storage;
  IScheme_Ptr _scheme;
};

void Aggregator::aggregate(const std::string &from_interval,
                           const std::string &to_interval, IEngine_Ptr _storage,
                           Time start, Time end) {

  logger_info("aggregator: from: '", from_interval, "' to:'", to_interval, "'[",
              timeutil::to_string(start), ", ", timeutil::to_string(end), "]");
  if (_storage == nullptr) {
    logger("aggregator: call with null storage");
    return;
  }

  auto scheme = _storage->getScheme();
  if (scheme == nullptr) {
    logger_fatal("aggregator: call with null scheme");
    return;
  }

  auto interval_values = scheme->lsInterval(from_interval);
  logger("aggregator: find ", interval_values.size(), " values.");

  for (auto kv : interval_values) {
    auto all_linked = scheme->linkedForValue(kv.second);

    dariadb::scheme::DescriptionMap linked_for_interval;

    for (auto l : all_linked) {
      if (l.second.interval == to_interval) {
        linked_for_interval.insert(std::make_pair(l.first, l.second));
      }
    }

    if (linked_for_interval.empty()) {
      continue;
    }

    std::vector<Id> target_ids;
    target_ids.reserve(linked_for_interval.size());
    std::vector<std::string> statistic_functions;
    statistic_functions.reserve(linked_for_interval.size());

    std::stringstream ss;
    for (auto l : linked_for_interval) {
      ss << " " << l.second.name;
      target_ids.push_back(l.second.id);
      statistic_functions.push_back(l.second.aggregation_func);
    }

    logger_info("aggregator: write '", kv.second.name, "' to '", ss.str(), "'");

    QueryInterval qi({kv.second.id}, dariadb::Flag(), start, end);
    auto values_from_to = _storage->readInterval(qi);

    statistic::Calculator calc(_storage);
    auto result_functions =
        calc.apply(kv.second.id, start, end, dariadb::Flag(), statistic_functions);
    ENSURE(result_functions.size() == target_ids.size());
    for (size_t i = 0; i < target_ids.size(); ++i) {
      _storage->append(target_ids[i], result_functions[i].time,
                       result_functions[i].value);
    }
  }
}

Aggregator::Aggregator(IEngine_Ptr storage)
    : _Impl(std::make_unique<Aggregator::Private>(storage)) {}

Aggregator::Aggregator(IEngine_Ptr storage, ITimer_Ptr timer)
    : _Impl(std::make_unique<Aggregator::Private>(storage, timer)) {}

Aggregator::~Aggregator() {
  _Impl = nullptr;
}