#include "metrics.h"
#include <iomanip>

using namespace dariadb::utils;

std::unique_ptr<MetricsManager> MetricsManager::_instance = nullptr;
#ifndef  MSVC
template <> 
std::string TemplateMetric<std::chrono::nanoseconds>::to_string() const {
	std::stringstream ss{};

	auto minc = std::chrono::duration<double, std::milli>(_min).count();
	auto maxc = std::chrono::duration<double, std::milli>(_max).count();
	auto avc = std::chrono::duration<double, std::milli>(_average).count();

	{
		std::stringstream subss{};
		subss << "| cnt:" << _count;
		ss << std::left << std::setw(METRIC_PARAM_WIDTH) << std::setfill(METRIC_FIELD_SEPARATOR) << subss.str();
	}
	{
		std::stringstream subss{};
		subss << "| min:" << minc << "ms";
		ss << std::left << std::setw(METRIC_PARAM_WIDTH) << std::setfill(METRIC_FIELD_SEPARATOR) << subss.str();
	}
	{
		std::stringstream subss{};
		subss << "| max:" << maxc << "ms";
		ss << std::left << std::setw(METRIC_PARAM_WIDTH) << std::setfill(METRIC_FIELD_SEPARATOR) << subss.str();
	}
	{
		std::stringstream subss{};
		subss << "| aver:" << avc << "ms";
		ss << std::left << std::setw(METRIC_PARAM_WIDTH) << std::setfill(METRIC_FIELD_SEPARATOR) << subss.str();
	}
	return ss.str();
}
#endif

dariadb::utils::MetricsManager::~MetricsManager() {}

MetricsManager *MetricsManager::instance() {
  if (MetricsManager::_instance == nullptr) {
    _instance = std::unique_ptr<MetricsManager>(new MetricsManager());
  }
  return _instance.get();
}

void MetricsManager::add(const std::string &group, const std::string &name,
                         const Metric_Ptr &value) {
  std::lock_guard<Locker> lg(_locker);

  auto group_it = this->_values.find(group);
  if (group_it == this->_values.end()) {
    this->_values[group].insert(std::make_pair(name, value));
  } else {
    auto name_it = (*group_it).second.find(name);
    if (name_it == (*group_it).second.end()) {
      (*group_it).second.insert(std::make_pair(name, value));
    } else {
      name_it->second->add(value);
    }
  }
}

std::string MetricsManager::to_string() const {
  const int nameWidth = 35;

  std::stringstream ss{};
  for (auto &grp_kv : _values) {
    ss << grp_kv.first << ":" << std::endl;
    for (auto &name_kv : grp_kv.second) {
      ss << std::left << std::setw(nameWidth) << std::setfill(METRIC_FIELD_SEPARATOR) << name_kv.first + ":"
		  << name_kv.second->to_string() << std::endl;
    }
	ss << std::endl;
  }
  return ss.str();
}
