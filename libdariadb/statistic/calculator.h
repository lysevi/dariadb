#pragma once

#include <libdariadb/interfaces/iengine.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/statistic/ifunction.h>
#include <istream>
#include <ostream>

namespace dariadb {
namespace statistic {

class FunctionFactory {
public:
  EXPORT static IFunction_ptr make_one(const std::string &k);
  EXPORT static std::vector<IFunction_ptr> make(const std::vector<std::string> &kinds);
  /***
  return vector of available functions.
  */
  EXPORT static std::vector<std::string> functions();
};

class Calculator {
public:
  EXPORT Calculator(const IEngine_Ptr &storage);
  EXPORT MeasArray apply(const IdArray &ids, Time from, Time to, Flag f,
                         const std::vector<std::string> &functions, const MeasArray &ma);

protected:
  IEngine_Ptr _storage;
};
} // namespace statistic
} // namespace dariadb