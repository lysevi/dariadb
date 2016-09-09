#pragma once

#include <cstdint>
#include <ostream>
#include <string>

namespace dariadb {
namespace net {

enum class DataKinds : uint8_t {
  OK,
  ERR,
  HELLO,
  DISCONNECT,
  PING,
  PONG,
  WRITE,
  READ_INTERVAL
};

enum class ClientState {
  CONNECT, // connection is beginning but a while not ended.
  WORK,    // normal client.
  DISCONNECTED
};

std::ostream &operator<<(std::ostream &stream, const ClientState &state);

typedef uint32_t QueryNumber;
}
}
