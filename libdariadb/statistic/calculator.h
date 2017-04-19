#pragma once

#include <libdariadb/interfaces/iengine.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/statistic/ifunction.h>
#include <istream>
#include <ostream>

namespace dariadb {
namespace statistic {

enum class FUNCTION_KIND : int { AVERAGE, MEDIAN, PERCENTILE90, PERCENTILE99 };

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

template <FUNCTION_KIND funckind, int percentile> class Percentile : public IFunction {
public:
  Percentile() {}
  void apply(const Meas &ma) override { _result.push_back(ma); }
  Meas result() const override {
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

    std::sort(_result.begin(), _result.end(), meas_value_compare_less());
    auto perc_float = percentile * 0.01;
    auto index = (size_t)(perc_float * _result.size());
    return _result.at(index);
  }

  int kind() const override { return (int)funckind; }

protected:
  mutable MeasArray _result;
};

using Median = Percentile<FUNCTION_KIND::MEDIAN, 50>;
using Percentile90 = Percentile<FUNCTION_KIND::PERCENTILE90, 90>;
using Percentile99 = Percentile<FUNCTION_KIND::PERCENTILE99, 99>;

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