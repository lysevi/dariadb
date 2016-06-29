#pragma once

#include "exception.h"
#include "locker.h"
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>

#ifdef ENABLE_METRICS
#define TIMECODE_METRICS(var, grp, name) dariadb::utils::RAI_TimeMetric var(grp, name);
#define ADD_METRICS(grp, name, m)                                                        \
  dariadb::utils::MetricsManager::instance()->add(grp, name, m);
#else
#define TIMECODE_METRICS(var, grp, name)
#define ADD_METRICS(grp, name, m)
#endif

namespace dariadb {
namespace utils {
	const char METRIC_FIELD_SEPARATOR = ' ';
	const int  METRIC_PARAM_WIDTH = 15;
class Metric;
using Metric_Ptr = std::shared_ptr<Metric>;
class Metric {
public:
  virtual void add(const Metric_Ptr &other) = 0;
  virtual std::string to_string() const = 0;
};

template <class T> class TemplateMetric : public Metric {
public:
  TemplateMetric(const T &value)
      : _value(value), _average(value), _min(value), _max(value), _count(1){}

  // Inherited via Metric
  virtual void add(const Metric_Ptr &other) override {
    auto other_raw = dynamic_cast<TemplateMetric *>(other.get());
    if (other_raw == nullptr) {
      throw MAKE_EXCEPTION("other_raw == nullptr");
    }
    _min = std::min(other_raw->_value, _min);
    _max = std::max(other_raw->_value, _max);
    _average = (_average + other_raw->_value) / 2;
    _count++;
  }

  virtual std::string to_string() const override {
    std::stringstream ss{};

	{
		std::stringstream subss{};
		subss << "| cnt:" << _count;
		ss  << std::left << std::setw(METRIC_PARAM_WIDTH) << std::setfill(METRIC_FIELD_SEPARATOR) << subss.str();
	}
	{
		std::stringstream subss{};
		subss << "| min:" << _min;
		ss << std::left << std::setw(METRIC_PARAM_WIDTH) << std::setfill(METRIC_FIELD_SEPARATOR) << subss.str();
	}
	{
		std::stringstream subss{};
		subss << "| max:" << _max;
		ss << std::left << std::setw(METRIC_PARAM_WIDTH) << std::setfill(METRIC_FIELD_SEPARATOR) << subss.str();
	}
	{
		std::stringstream subss{};
		subss << "| aver:" << _average;
		ss << std::left << std::setw(METRIC_PARAM_WIDTH) << std::setfill(METRIC_FIELD_SEPARATOR) << subss.str();
	}
    return ss.str();
  }

protected:
  T _value;
  T _average, _min, _max;
  size_t _count;
};

template <> std::string TemplateMetric<std::chrono::nanoseconds>::to_string() const;

using FloatMetric = TemplateMetric<float>;
using TimeMetric = TemplateMetric<std::chrono::nanoseconds>;

class MetricsManager {
protected:
  MetricsManager() = default;

public:
  static MetricsManager *instance();
  ~MetricsManager();
  void add(const std::string &group, const std::string &name, const Metric_Ptr &value);
  std::string to_string() const;

protected:
  static std::unique_ptr<MetricsManager> _instance;
  using NameToValue = std::map<std::string, Metric_Ptr>;
  using GroupToName = std::unordered_map<std::string, NameToValue>;

  GroupToName _values;
  utils::Locker _locker;
};

class RAI_TimeMetric {
public:
  RAI_TimeMetric(const std::string &group, const std::string &name) {
    _value = std::chrono::high_resolution_clock::now();
    _group = group;
    _name = name;
  }
  ~RAI_TimeMetric() {
    auto elapsed = std::chrono::high_resolution_clock::now() - _value;
    MetricsManager::instance()->add(_group, _name, Metric_Ptr{new TimeMetric(elapsed)});
  }

protected:
  std::chrono::high_resolution_clock::time_point _value;
  std::string _group;
  std::string _name;
};
}
}
