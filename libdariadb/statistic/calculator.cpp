#include <libdariadb/statistic/calculator.h>
#include <libdariadb/utils/utils.h>
using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::statistic;

/// value deccreasing.
struct meas_value_compare_less {
	bool operator()(const dariadb::Meas &lhs, const dariadb::Meas &rhs) const {
		return lhs.value < rhs.value;
	}
};


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

Median::Median() {}

void Median::apply(const Meas &ma) {
  _result.push_back(ma);
}

Meas Median::result() const {
  if (_result.empty()) {
    return Meas();
  }

  if (_result.size() == 1) {
    return _result.front();
  }

  if (_result.size() == 2) {
    Meas m = _result[0];
    m.value += _result[1].value;
    m.value /= 2;
    return m;
  }
  
  auto mid = _result.begin();
  std::advance(mid, _result.size() / 2);
  std::partial_sort(_result.begin(), mid + 1, _result.end(), meas_value_compare_less());
  return *mid;
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
    }
  }
  return result;
}

FunctionKinds FunctionFactory::functions() {
  return {FUNCTION_KIND::AVERAGE, FUNCTION_KIND::MEDIAN};
}

Calculator::Calculator(const IEngine_Ptr &storage) : _storage(storage) {}

MeasArray Calculator::apply(const IdArray &ids, Time from, Time to, Flag f,
                            const FunctionKinds &functions, const MeasArray &ma) {
  return MeasArray();
}