#include <libdariadb/aggregate/aggregator.h>
#include <libdariadb/statistic/calculator.h>
#include <libdariadb/timeutil.h>
#include <sstream>

using namespace dariadb;
using namespace dariadb::aggregator;
using namespace dariadb::storage;
using namespace dariadb::timeutil;

class Aggreagator::Private {
public:
};

void Aggreagator::aggregate(const std::string &from_interval,
                            const std::string &to_interval, IEngine_Ptr _storage,
                            Time start, Time end) {

  logger("aggregator: from: ", from_interval, " to:", to_interval, "[",
         timeutil::to_string(start), ", ", timeutil::to_string(end), "]");
  if (_storage == nullptr) {
    logger("aggregator: call with null storage");
    return;
  }

  auto scheme = _storage->getScheme();
  if (scheme == nullptr) {
    logger("aggregator: call with null scheme");
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

    logger("aggregator: write ", kv.second.name, " to ", ss.str());

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

Aggreagator::Aggreagator() : _Impl(std::make_unique<Aggreagator::Private>()) {}