#pragma once

#include <libdariadb/meas.h>
#include <ostream>
#include <string>
#include <unordered_map>

namespace dariadb {
namespace scheme {
struct MeasurementDescription {
  std::string name;
  Id id;
  std::string interval;
  std::string aggregation_func;

  // return value without interval.
  std::string prefix() const {
    if (interval == "") {
      return name;
    }
    auto interval_pos = name.find(interval);
    if (interval_pos == std::string::npos) {
      return name;
    }
    return name.substr(0, interval_pos - 1);
  }
};

class DescriptionMap : public std::unordered_map<Id, MeasurementDescription> {
public:
  Id idByParam(const std::string &param) {
    auto fres = std::find_if(begin(), end(),
                             [&param](auto kv) { return kv.second.name == param; });
    if (fres == end()) {
      return MAX_ID;
    } else {
      return fres->first;
    }
  }
};

class IScheme {
public:
  virtual Id addParam(const std::string &param) = 0;
  virtual DescriptionMap ls() = 0;
  virtual DescriptionMap lsInterval(const std::string &interval) = 0;
  virtual DescriptionMap linkedForValue(const MeasurementDescription &param) = 0;
  virtual void save() = 0;
};

using IScheme_Ptr = std::shared_ptr<IScheme>;
} // namespace scheme
} // namespace dariadb
