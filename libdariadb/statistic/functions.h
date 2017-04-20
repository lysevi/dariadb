#pragma once

#include <libdariadb/st_exports.h>
#include <libdariadb/statistic/ifunction.h>

namespace dariadb {
namespace statistic {

class Average : public IFunction {
public:
  EXPORT Average(const std::string &s);
  EXPORT void apply(const Meas &ma) override;
  EXPORT Meas result() const override;

protected:
  Meas _result;
  size_t _count;
};

template <int percentile> class Percentile : public IFunction {
public:
  Percentile(const std::string &s) : IFunction(s) {}

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

protected:
  mutable MeasArray _result;
};

using Median = Percentile<50>;
using Percentile90 = Percentile<90>;
using Percentile99 = Percentile<99>;
} // namespace statistic
} // namespace dariadb