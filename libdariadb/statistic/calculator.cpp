#include <libdariadb/statistic/calculator.h>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::statistic;

std::istream &dariadb::statistic::operator>>(std::istream &in, FUNCKTION_KIND &f) {
  std::string token;
  in >> token;

  token = utils::strings::to_upper(token);
  if (token == "AVERAGE") {
    f = FUNCKTION_KIND::AVERAGE;
    return in;
  }
  return in;
}

std::ostream &dariadb::statistic::operator<<(std::ostream &stream,
                                             const FUNCKTION_KIND &f) {
  switch (f) {
  case FUNCKTION_KIND::AVERAGE:
    stream << "average";
    break;
  }
  return stream;
}

Average::Average() : _result() {}
void Average::apply(const Meas &ma) {}

Meas Average::result() const {
  return _result;
}

std::vector<IFunction_ptr>
FunctionFactory::make(const std::vector<FUNCKTION_KIND> &kinds) {
  std::vector<IFunction_ptr> result(kinds.size());
  size_t i = 0;
  for (const auto k : kinds) {
    switch (k) {
    case FUNCKTION_KIND::AVERAGE:
      result[i] = std::make_shared<Average>();
      break;
    }
  }
  return result;
}

Calculator::Calculator(const IEngine_Ptr &storage) : _storage(storage) {}

MeasArray Calculator::apply(const IdArray &ids, Time from, Time to, Flag f,
                            const std::vector<FUNCKTION_KIND> &functions,
                            const MeasArray &ma) {
  return MeasArray();
}