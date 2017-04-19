#pragma once

#include <libdariadb/interfaces/iengine.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/statistic/ifunction.h>
#include <istream>
#include <ostream>

namespace dariadb {
namespace statistic {

enum class FUNCTION_KIND : int { AVERAGE, MEDIAN };

using FunctionKinds = std::vector<FUNCTION_KIND>;

class Average : public IFunction {
public:
  EXPORT Average();
  EXPORT void apply(const Meas &ma) override;
  EXPORT Meas result() const override;
  int kind() const override { return (int)FUNCTION_KIND::AVERAGE; }

protected:
  Meas _result;
  size_t _count;
};

class Median : public IFunction {
public:
  EXPORT Median();
  EXPORT void apply(const Meas &ma) override;
  EXPORT Meas result() const override;
  int kind() const override { return (int)FUNCTION_KIND::MEDIAN; }

protected:
  mutable MeasArray _result;
};

EXPORT std::istream &operator>>(std::istream &in, FUNCTION_KIND &f);
EXPORT std::ostream &operator<<(std::ostream &stream, const FUNCTION_KIND &f);

class FunctionFactory {
public:
  EXPORT static std::vector<IFunction_ptr> make(const FunctionKinds &kinds);
  /***
  return vector of available functions.
  */
  EXPORT static FunctionKinds functions();
};

class Calculator {
public:
  EXPORT Calculator(const IEngine_Ptr &storage);
  EXPORT MeasArray apply(const IdArray &ids, Time from, Time to, Flag f,
                         const FunctionKinds &functions, const MeasArray &ma);

protected:
  IEngine_Ptr _storage;
};
} // namespace statistic
} // namespace dariadb