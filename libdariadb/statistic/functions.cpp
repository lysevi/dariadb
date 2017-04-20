#include <libdariadb/statistic/functions.h>
#include <cmath>

using namespace dariadb;
using namespace dariadb::statistic;

Average::Average(const std::string &s) : IFunction(s), _result(), _count() {}

void Average::apply(const Meas &ma) {
  _result.id = ma.id;
  _result.value += ma.value;
  _count++;
}

Meas Average::result() const {
  if (_count == 0) {
    return Meas();
  }
  Meas m = _result;
  m.value = m.value / _count;
  return m;
}

StandartDeviation::StandartDeviation(const std::string &s) : IFunction(s) {}

void StandartDeviation::apply(const Meas &m) {
  ma.push_back(m);
}

Meas StandartDeviation::result() const {
  if (ma.empty()) {
    return Meas();
  }

  auto collection_size = ma.size();

  Value average_value = Value();
  for (auto m : ma) {
    average_value += m.value;
  }
  average_value = average_value / collection_size;

  Value sum_value = Value();
  for (auto m : ma) {
    sum_value += std::pow(m.value - average_value, 2);
  }
  Meas result;
  result.value = std::sqrt(sum_value / collection_size);
  return result;
}