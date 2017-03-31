#pragma once

#include <string>

namespace dariadb {
namespace net {
namespace http {

struct header {
  std::string name;
  std::string value;
};

} // namespace http
} // namespace net
} // namespace dariadb