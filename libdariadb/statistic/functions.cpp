#include <libdariadb/statistic/functions.h>
#include <cmath>

using namespace dariadb;
using namespace dariadb::statistic;

Average::Average(const std::string &s) : IFunction(s) {}

Meas Average::apply(const MeasArray &ma) {
  Meas result;
  size_t count = 0;
  for (auto m : ma) {
    result.id = m.id;
    result.value += m.value;
    result.time = std::max(result.time, m.time);
    count++;
  }
  if (count == 0) {
    return Meas();
  }
  Meas m = result;
  m.value = m.value / count;
  return m;
}

StandartDeviation::StandartDeviation(const std::string &s) : IFunction(s) {}

Meas StandartDeviation::apply(const MeasArray &ma) {
  if (ma.empty()) {
    return Meas();
  }
  auto collection_size = ma.size();

  Value average_value = Value();
  Time maxtime = MIN_TIME;
  for (auto m : ma) {
    average_value += m.value;
    maxtime = std::max(maxtime, m.time);
  }
  average_value = average_value / collection_size;

  Value sum_value = Value();
  for (auto m : ma) {
    sum_value += std::pow(m.value - average_value, 2);
  }
  Meas result;
  result.value = std::sqrt(sum_value / collection_size);
  result.time = maxtime;
  return result;
}