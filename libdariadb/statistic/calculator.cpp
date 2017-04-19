#include <libdariadb/statistic/calculator.h>
#include <libdariadb/statistic/functions.h>
#include <libdariadb/utils/utils.h>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::statistic;

IFunction_ptr FunctionFactory::make_one(const FUNCTION_KIND k) {
  switch (k) {
  case FUNCTION_KIND::AVERAGE:
    return std::make_shared<Average>();
  case FUNCTION_KIND::MEDIAN:
    return std::make_shared<Median>();
  case FUNCTION_KIND::PERCENTILE90:
    return std::make_shared<Percentile90>();
  case FUNCTION_KIND::PERCENTILE99:
    return std::make_shared<Percentile99>();
  }
  return nullptr;
}

std::vector<IFunction_ptr> FunctionFactory::make(const FunctionKinds &kinds) {
  std::vector<IFunction_ptr> result(kinds.size());
  size_t i = 0;
  for (const auto k : kinds) {
    result[i++] = make_one(k);
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