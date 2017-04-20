#include <libdariadb/flags.h>
#include <libdariadb/statistic/calculator.h>
#include <libdariadb/statistic/functions.h>
#include <libdariadb/utils/utils.h>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::statistic;

IFunction_ptr FunctionFactory::make_one(const std::string &k) {
  auto token = utils::strings::to_lower(k);
  if (token == "average") {
    return std::make_shared<Average>("average");
  }
  if (token == "median") {
    return std::make_shared<Median>("median");
  }
  if (token == "percentile90") {
    return std::make_shared<Percentile90>("percentile90");
  }
  if (token == "percentile99") {
    return std::make_shared<Percentile99>("percentile99");
  }
  if (token == "sigma") {
    return std::make_shared<StandartDeviation>("sigma");
  }
  return nullptr;
}

std::vector<IFunction_ptr> FunctionFactory::make(const std::vector<std::string> &kinds) {
  std::vector<IFunction_ptr> result(kinds.size());
  size_t i = 0;
  for (const auto k : kinds) {
    result[i++] = make_one(k);
  }
  return result;
}

std::vector<std::string> FunctionFactory::functions() {
  return {"average", "median", "percentile90", "percentile99", "sigma"};
}

Calculator::Calculator(const IEngine_Ptr &storage) : _storage(storage) {}

MeasArray Calculator::apply(const Id id, Time from, Time to, Flag flag,
                            const std::vector<std::string> &functions) {

  dariadb::QueryInterval qi({id}, flag, from, to);
  auto ma = _storage->readInterval(qi);
  if (ma.empty()) {
    return MeasArray();
  }
  auto all_functions = FunctionFactory::make(functions);
  for (size_t i = 0; i < all_functions.size(); ++i) {
    if (all_functions[i] == nullptr) {
      logger_fatal("unknow function '", functions[i], "'");
    }
  }
  for (auto m : ma) {
    for (auto f : all_functions) {
      if (f != nullptr) {
        f->apply(m);
      }
    }
  }
  MeasArray result;
  result.reserve(functions.size());
  for (auto f : all_functions) {
    Meas m;
    m.id = id;
    if (f != nullptr) {
      m.value = f->result().value;
    }
    result.push_back(m);
  }
  return result;
}