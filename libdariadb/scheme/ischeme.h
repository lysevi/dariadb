#pragma once

#include <list>
#include <string>
#include <libdariadb/meas.h>

namespace dariadb {
namespace scheme {
struct MeasurementDescription {
	std::string name;
	Id id;
};
class IScheme {
public:
  virtual Id addParam(const std::string &param) = 0;
  virtual std::list<MeasurementDescription> ls() = 0;
  virtual std::list<MeasurementDescription> ls(const std::string &pattern) = 0;
};
}
}