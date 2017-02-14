#pragma once

#include <libdariadb/st_exports.h>
#include <string>

namespace dariadb{
namespace scheme{
namespace helpers{
	EXPORT bool isParamInPatter(const std::string&param, const std::string&patter);
}
}
}