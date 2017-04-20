#include <libdariadb/statistic/functions.h>

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
