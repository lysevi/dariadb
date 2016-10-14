#pragma once

#include <cstdint>
#include <ostream>
#include <string>

namespace dariadb {
namespace net {

const uint32_t PROTOCOL_VERSION = 1;

enum class CLIENT_STATE {
  CONNECT, // connection is beginning but a while not ended.
  WORK,    // normal client.
  DISCONNECTED
};

enum class ERRORS : uint16_t {
  WRONG_PROTOCOL_VERSION,
  WRONG_QUERY_PARAM_FROM_GE_TO, // if in readInterval from>=to
};

std::ostream &operator<<(std::ostream &stream, const CLIENT_STATE &state);
std::ostream &operator<<(std::ostream &stream, const ERRORS &state);

typedef int32_t QueryNumber;
}
}
