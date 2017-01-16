#include <boost/asio.hpp>
#include <common/net_common.h>
#include <istream>
#include <ostream>

namespace dariadb {
namespace net {
std::ostream &operator<<(std::ostream &stream, const CLIENT_STATE &state) {
  switch (state) {
  case dariadb::net::CLIENT_STATE::WORK:
    stream << "CLIENT_STATE::WORK";
    break;
  case dariadb::net::CLIENT_STATE::DISCONNECTED:
    stream << "CLIENT_STATE::DISCONNECTED";
    break;
  case dariadb::net::CLIENT_STATE::DISCONNETION_START:
    stream << "CLIENT_STATE::DISCONNETION_START";
    break;
  case dariadb::net::CLIENT_STATE::CONNECT:
    stream << "CLIENT_STATE::CONNECT";
    break;
  }
  return stream;
}

std::ostream &operator<<(std::ostream &stream, const ERRORS &state) {
  switch (state) {
  case dariadb::net::ERRORS::WRONG_PROTOCOL_VERSION:
    stream << "ERRORS::WRONG_PROTOCOL_VERSION";
    break;
  case dariadb::net::ERRORS::WRONG_QUERY_PARAM_FROM_GE_TO:
    stream << "ERRORS::WRONG_QUERY_PARAM_FROM_GE_TO";
    break;
  case dariadb::net::ERRORS::APPEND_ERROR:
    stream << "ERRORS::APPEND_ERROR";
    break;
  }
  return stream;
}

std::string to_string(const CLIENT_STATE &st) {
  std::stringstream ss;
  ss << st;
  return ss.str();
}

std::string to_string(const ERRORS &st) {
  std::stringstream ss;
  ss << st;
  return ss.str();
}
}
}
