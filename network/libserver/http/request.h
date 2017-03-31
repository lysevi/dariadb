#pragma once
#include <libserver/http/header.h>
#include <string>
#include <vector>

namespace dariadb {
namespace net {
namespace http {

/// A request received from a client.
struct request {
  std::string method;
  std::string uri;
  int http_version_major;
  int http_version_minor;
  std::vector<header> headers;

  std::string query; // all values after headers.
};

} // namespace http
} // namespace net
} // namespace dariadb