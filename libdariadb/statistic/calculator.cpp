#include <libdariadb/statistic/calculator.h>
#include <libdariadb/utils/utils.h>
using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::statistic;

std::istream &dariadb::statistic::operator>>(std::istream &in, FUNCTION_KIND &f) {
  std::string token;
  in >> token;

  token = utils::strings::to_lower(token);
  if (token == "average") {
    f = FUNCTION_KIND::AVERAGE;
    return in;
  }
  if (token == "median") {
    f = FUNCTION_KIND::MEDIAN;
    return in;
  }
  if (token == "percentile90") {
    f = FUNCTION_KIND::PERCENTILE90;
    return in;
  }
  if (token == "percentile99") {
    f = FUNCTION_KIND::PERCENTILE99;
    return in;
  }
  return in;
}

std::ostream &dariadb::statistic::operator<<(std::ostream &stream,
                                             const FUNCTION_KIND &f) {
  switch (f) {
  case FUNCTION_KIND::AVERAGE:
    stream << "average";
    break;
  case FUNCTION_KIND::MEDIAN:
    stream << "median";
    break;
  case FUNCTION_KIND::PERCENTILE90:
    stream << "percentile90";
    break;
  case FUNCTION_KIND::PERCENTILE99:
    stream << "percentile99";
    break;
  }

  return stream;
}

Average::Average() : _result(), _count() {}

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

std::vector<IFunction_ptr> FunctionFactory::make(const FunctionKinds &kinds) {
  std::vector<IFunction_ptr> result(kinds.size());
  size_t i = 0;
  for (const auto k : kinds) {
    switch (k) {
    case FUNCTION_KIND::AVERAGE:
      result[i] = std::make_shared<Average>();
      break;
    case FUNCTION_KIND::MEDIAN:
      result[i] = std::make_shared<Median>();
      break;
    case FUNCTION_KIND::PERCENTILE90:
      result[i] = std::make_shared<Percentile90>();
      break;
    case FUNCTION_KIND::PERCENTILE99:
      result[i] = std::make_shared<Percentile99>();
      break;
    }
	i++;
  }
  return result;
}

FunctionKinds FunctionFactory::functions() {
  return {FUNCTION_KIND::AVERAGE, FUNCTION_KIND::MEDIAN, FUNCTION_KIND::PERCENTILE90,
          FUNCTION_KIND::PERCENTILE99};
}

Calculator::Calculator(const IEngine_Ptr &storage) : _storage(storage) {}

MeasArray Calculator::apply(const IdArray &ids, Time from, Time to, Flag f,
                            const FunctionKinds &functions, const MeasArray &ma) {
  return MeasArray();
}