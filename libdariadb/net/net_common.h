#pragma once

#include <ostream>
#include <string>
#include <cstdint>

namespace dariadb {
namespace net {

    enum class DataKinds:uint8_t{
        OK_ANSWER,
        ERROR_ANSWER,
        HELLO_PREFIX,
        DISCONNECT_PREFIX,
        DISCONNECT_ANSWER,
        PING_QUERY,
        PONG_ANSWER,
        WRITE_QUERY,
        READ_INTERVAL_QUERY
    };

enum class ClientState {
  CONNECT, // connection is beginning but a while not ended.
  WORK,    // normal client.
  DISCONNECTED
};

std::ostream &operator<<(std::ostream &stream, const ClientState &state);
}
}
