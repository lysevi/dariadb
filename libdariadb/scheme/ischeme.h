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

class DescriptionMap: public std::unordered_map<Id, MeasurementDescription>
{
public:
	Id idByParam(const std::string&param)
	{
		auto fres = std::find_if(begin(), end(), [&param](auto kv) {return kv.second.name == param; });
		if (fres == end()) {
			return MAX_ID;
		}
		else {
			return fres->first;
		}
	}
};

class IScheme {
public:
  virtual Id addParam(const std::string &param) = 0;
  virtual DescriptionMap ls() = 0;
};
}
}
