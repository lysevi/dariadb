#pragma once

#include <boost/asio.hpp>
#include <common/net_cmn_exports.h>
#include <string>

namespace dariadb {
namespace net {
namespace http {
struct http_response {
  int code;
  std::string answer;
};

CM_EXPORT http_response POST(boost::asio::io_service &service, const std::string &port,
                             const std::string &json_query);
CM_EXPORT http_response GET(boost::asio::io_service &service, const std::string &port,
                            const std::string &path);
} // namespace http
} // namespace net
} // namespace dariadb