#include "net_common.h"
#include <boost/asio.hpp>
#include <istream>

namespace dariadb {
namespace net {
std::ostream &operator<<(std::ostream &stream, const ClientState &state) {
  switch (state) {
  case dariadb::net::ClientState::WORK:
    stream << "ClientState::WORK";
    break;
  case dariadb::net::ClientState::DISCONNECTED:
    stream << "ClientState::DISCONNECTED";
    break;
  case dariadb::net::ClientState::CONNECT:
    stream << "ClientState::CONNECT";
    break;
  }
  return stream;
}

std::ostream &operator<<(std::ostream &stream, const ERRORS &state){
    switch (state) {
    case dariadb::net::ERRORS::WRONG_PROTOCOL_VERSION:
      stream << "ERRORS::WRONG_PROTOCOL_VERSION";
      break;
    }
    return stream;
}
}
}
