#pragma once

#include <libdariadb/st_exports.h>
#include <libdariadb/statistic/ifunction.h>

namespace dariadb {
namespace statistic {

class Average : public IFunction {
public:
  EXPORT Average(const std::string &s);
  EXPORT Meas apply(const MeasArray &ma) override;
};

class Minimum : public IFunction {
public:
	EXPORT Minimum(const std::string &s);
	EXPORT Meas apply(const MeasArray &ma) override;
};

class Maximum : public IFunction {
public:
    EXPORT Maximum(const std::string &s);
    EXPORT Meas apply(const MeasArray &ma) override;
};

class StandartDeviation : public IFunction {
public:
	EXPORT StandartDeviation(const std::string &s);
	EXPORT Meas apply(const MeasArray &ma) override;
};

template <int percentile> class Percentile : public IFunction {
public:
  Percentile(const std::string &s) : IFunction(s) {}

  EXPORT Meas apply(const MeasArray &ma) override {
    if (ma.empty()) {
      return Meas();
    }

    if (ma.size() == 1) {
      return ma.front();
    }

    if (ma.size() == 2) {
      Meas m = ma[0];
      m.value += ma[1].value;
      m.value /= 2;
      m.time = std::max(ma[0].time, ma[1].time);
      return m;
    }
    MeasArray _result(ma);
    std::sort(_result.begin(), _result.end(), meas_value_compare_less());
    auto perc_float = percentile * 0.01;
    auto index = (size_t)(perc_float * _result.size());
    return _result.at(index);
  }
}; // namespace statistic

using Median = Percentile<50>;
using Percentile90 = Percentile<90>;
using Percentile99 = Percentile<99>;
} // namespace statistic
} // namespace dariadb
