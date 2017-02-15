#pragma once

#include <unordered_map>
#include <string>
#include <libdariadb/meas.h>
#include <ostream>

namespace dariadb {
namespace scheme {
struct MeasurementDescription {
	std::string name;
	Id id;
};
using DescriptionMap = std::unordered_map<Id, MeasurementDescription>;
class IScheme {
public:
  virtual Id addParam(const std::string &param) = 0;
  virtual DescriptionMap ls() = 0;
  virtual DescriptionMap ls(const std::string &pattern) = 0;
};
}
}